import pika
import json
import requests
import time
import datetime
import pytz
import os
import threading
from dotenv import load_dotenv
from pathlib import Path

class RabbitMQConsumerThread(threading.Thread):
    def __init__(
        self,
        thread_id,
        host_name,
        host_port,
        pika_user,
        pika_pass,
        queue_name,
        max_retries,
        stage_program,
        internal_url_hit
    ):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.host_name = host_name
        self.host_port = host_port
        self.pika_user = pika_user
        self.pika_pass = pika_pass
        self.queue_name = queue_name
        self.max_retries = max_retries
        self.stage_program = stage_program
        self.internal_url_hit = internal_url_hit


    def run(self):
        JakartaTz = pytz.timezone("Asia/Jakarta")

        print(
            f'[*] {datetime.datetime.now(JakartaTz)} '
            f'Connecting to RabbitMQ... (Thread {self.thread_id})',
            flush=True
        )

        credentials = pika.PlainCredentials(self.pika_user, self.pika_pass)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host_name,
                port=int(self.host_port),
                virtual_host='/',
                credentials=credentials
            )
        )

        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=True)
        
        # Declare Final DLQ
        final_dlq_name = self.queue_name + "_dlq_final"
        channel.queue_declare(queue=final_dlq_name, durable=True)
        
        # Declare Retry Queue with DLX back to Main Queue
        retry_queue_name = self.queue_name + "_retry"
        args = {
            'x-message-ttl': 10000,
            'x-dead-letter-exchange': '',
            'x-dead-letter-routing-key': self.queue_name
        }
        channel.queue_declare(queue=retry_queue_name, durable=True, arguments=args)

        print(
            f'[*] {datetime.datetime.now(JakartaTz)} '
            f'Waiting for messages on {self.queue_name} '
            f'(Thread {self.thread_id})',
            flush=True
        )

        def callback(ch, method, properties, body):
            print(
                f"[x] {datetime.datetime.now(JakartaTz)} "
                f"Received: {body} (Thread {self.thread_id})",
                flush=True
            )

            try:
                data = json.loads(body)
            except Exception as e:
                print(f"[ERROR] Invalid JSON: {e}", flush=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            if "msgType" not in data or "msgInfo" not in data:
                print("[WARN] msgType / msgInfo not found", flush=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # ============================
            # NOTIFICATION TRANSFER
            # ============================
            if data["msgType"] == "consumer_notification_transfer":

                payload = json.dumps({
                    "msgInfo": data["msgInfo"]
                })

                headers = {
                    "Content-Type": "application/json"
                }

                try:
                    response = requests.post(
                        self.internal_url_hit + "/Notification/Transfer",
                        headers=headers,
                        data=payload
                    )
                    print("CALL API:", self.internal_url_hit + "/Notification/transfer", flush=True)
                    print("PAYLOAD:", payload, flush=True)

                    print(
                        f"[TRANSFER][THREAD:{self.thread_id}] "
                        f"HTTP {response.status_code} | "
                        f"BODY={response.text}",
                        flush=True
                    )


                    # ============================
                    # SUCCESS CALLBACK
                    # ============================
                    if response.status_code == 200:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        
                        # Hapus dari tabel DLQ monitoring jika sukses dan ini dari manual retry
                        if data.get('is_manual_retry') == True:
                            try:
                                ref_id = data.get('msgInfo', {}).get('ref_cashoutPaymentId')
                                if ref_id:
                                    del_payload = json.dumps({"type": "transfer", "ref_id": ref_id})
                                    requests.post(self.internal_url_hit+"/Notification/DeleteDlq", headers=headers, data=del_payload)
                            except Exception as e:
                                print(f"Error deleting DLQ: {e}")

                        # Cek dan kirim notifikasi jika merchant ini baru saja pulih
                        merchant_id = data.get('msgInfo', {}).get('merchantId') or data.get('msgInfo', {}).get('merchant_id')
                        check_and_send_recovery_alert(merchant_id, "transfer", self.internal_url_hit)

                    # ============================
                    # FAILED → RETRY
                    # ============================
                    else:
                        retry_count = data.get('retry_count', 0)
                        
                        if retry_count < self.max_retries:
                            data['retry_count'] = retry_count + 1
                            print(f"[!] {datetime.datetime.now(JakartaTz)} Failed (Attempt {retry_count+1}/{self.max_retries}). Routing to Retry Queue (Thread {self.thread_id})", flush=True)
                            
                            channel.basic_publish(
                                exchange='',
                                routing_key=self.queue_name + "_retry",
                                body=json.dumps(data),
                                properties=pika.BasicProperties(
                                    delivery_mode=2,
                                    priority=int(properties.priority) if properties.priority else 0,
                                    headers=properties.headers
                                )
                            )
                        else:
                            print(f"[!] {datetime.datetime.now(JakartaTz)} Max retries reached. Routing to Final DLQ (Thread {self.thread_id})", flush=True)
                            
                            # Inject response dan payload sebelum dilempar ke Final DLQ
                            data['merchant_response'] = response.text
                            data['payload'] = payload
                            
                            channel.basic_publish(
                                exchange='',
                                routing_key=self.queue_name + "_dlq_final",
                                body=json.dumps(data),
                                properties=pika.BasicProperties(
                                    delivery_mode=2,
                                    priority=int(properties.priority) if properties.priority else 0,
                                    headers=properties.headers
                                )
                            )
                        
                        ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    print(f"[ERROR] Callback exception: {e}", flush=True)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            else:
                print(
                    f"[INFO] Unsupported msgType: {data['msgType']}",
                    flush=True
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)

            print("", flush=True)

        channel.basic_qos(prefetch_count=1)

        channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback
        )

        channel.start_consuming()


# ============================
# LOAD ENV
# ============================
dotenv_path = Path('/etc/config_db/.env')
load_dotenv(dotenv_path=dotenv_path)

host_name = os.environ['host_name']
host_port = os.environ['host_port']
pika_user = os.environ['pika_user']
pika_pass = os.environ['pika_pass']
internal_url_hit = os.environ['internal_url_hit']
stage_program = os.environ.get('stage_program', 'development')

queue_name = "queue_notification_transfer"
max_retries = 3
num_threads = 5

# ============================
# START THREADS
# ============================
threads = []

for i in range(num_threads):
    t = RabbitMQConsumerThread(
        thread_id=i + 1,
        host_name=host_name,
        host_port=host_port,
        pika_user=pika_user,
        pika_pass=pika_pass,
        queue_name=queue_name,
        max_retries=max_retries,
        stage_program=stage_program,
        internal_url_hit=internal_url_hit
    )
    t.start()
    threads.append(t)

for t in threads:
    t.join()
