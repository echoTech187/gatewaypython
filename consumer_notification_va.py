import pika
import json
import requests
import time
import datetime
import pytz
import os
import threading
import hashlib
from urllib.parse import urlencode
from dotenv import load_dotenv
from pathlib import Path

# Lokasi file JSON untuk track last alert (digunakan bersama dengan DLQ Monitor)
ALERT_STATE_FILE = os.path.join(os.path.dirname(__file__), 'logs', 'last_alerted.json')

def check_and_send_recovery_alert(merchant_id, type_val, internal_url_hit):
    if not merchant_id:
        return
        
    merchant_id = str(merchant_id)
    if os.path.exists(ALERT_STATE_FILE):
        try:
            with open(ALERT_STATE_FILE, 'r') as f:
                alert_data = json.load(f)
            
            # Jika nilainya > 0, berarti sebelumnya sedang error dan dalam masa jeda
            if merchant_id in alert_data and alert_data[merchant_id] > 0:
                print(f"[+] Merchant {merchant_id} has recovered! Sending Telegram alert...", flush=True)
                
                # 1. Reset jeda agar tidak terkirim recovery berulang kali
                alert_data[merchant_id] = 0
                with open(ALERT_STATE_FILE, 'w') as f:
                    json.dump(alert_data, f)
                    
                # 2. Buat Token & Link Resend
                ts = int(time.time())
                secret = "ResendGatewaySecret2026!"
                raw_str = f"{merchant_id}{type_val}{ts}{secret}"
                token = hashlib.md5(raw_str.encode('utf-8')).hexdigest()
                
                query_params = {
                    'type': type_val,
                    'merchantId': merchant_id,
                    'ts': str(ts),
                    'token': token
                }
                new_query = urlencode(query_params)
                resend_link = f"{internal_url_hit}/Notification/BulkResend?{new_query}"
                
                # 3. Kirim pesan Telegram
                info_text = f"✅ <b>[RECOVERY] API Merchant Aktif Kembali</b>\n\n"
                info_text += f"Transaksi terbaru untuk Merchant: <b>{{merchantName}}</b> telah berhasil diproses dan layanan kembali normal.\n\n"
                info_text += f"Silakan tekan tombol di bawah ini untuk mengirim ulang (Flush) semua sisa transaksi yang gagal sebelumnya:\n"
                info_text += f"<a href='{resend_link}'>Eksekusi Bulk Resend (Direct API)</a>"
                
                payload_tg = json.dumps({"msgInfo": {"info_text": info_text, "merchantId": merchant_id}})
                headers_tg = {'Content-Type': 'application/json'}
                requests.post(internal_url_hit+"/Telegram/sendMessage", headers=headers_tg, data=payload_tg)
        except Exception as e:
            print(f"Error sending recovery alert: {e}", flush=True)

class RabbitMQConsumerThread(threading.Thread):
    def __init__(self, thread_id, host_name, host_port, pika_user, pika_pass, queue_name, max_retries, stage_program, internal_url_hit, external_url_hit):
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
        self.external_url_hit = external_url_hit

    def run(self):
        JakartaTz = pytz.timezone("Asia/Jakarta")

        print(f'[*] {datetime.datetime.now(JakartaTz)} Connecting to server ... (Thread {self.thread_id})', flush=True)

        credentials = pika.PlainCredentials(self.pika_user, self.pika_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.host_name, self.host_port, '/', credentials))
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

        print(f'[*] {datetime.datetime.now(JakartaTz)} Waiting for messages. (Thread {self.thread_id})', flush=True)

        def callback(ch, method, properties, body):
            print(f"[x] Received {datetime.datetime.now(JakartaTz)} [xx] {body} (Thread {self.thread_id})", flush=True)

            data = json.loads(body)

            if "msgType" in data and "msgInfo" in data:
                if data['msgType'] == "consumer_notification_va":
                    payload = json.dumps({
                        "msgInfo": data['msgInfo']
                    })

                    headers = {
                        'Content-Type': 'application/json'
                    }

                    response = requests.request("POST", internal_url_hit+"/Notification/Va", headers=headers, data=payload)

                    print(f"[Get Response Code] {datetime.datetime.now(JakartaTz)} [xx] {response.status_code} [xx] {data} (Thread {self.thread_id})", flush=True)
                    print(f"[Get Full Response] {datetime.datetime.now(JakartaTz)} [xx] {response.text} [xx] {data} (Thread {self.thread_id})", flush=True)

                    print("================ (Thread {self.thread_id})", flush=True)

                    if response.status_code == 200:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        
                        # Hapus dari tabel DLQ monitoring jika sukses dan ini dari manual retry
                        if data.get('is_manual_retry') == True:
                            try:
                                ref_id = data.get('msgInfo', {}).get('ref_cashinPaymentVaId')
                                if ref_id:
                                    del_payload = json.dumps({"type": "virtual-account", "ref_id": ref_id})
                                    requests.post(internal_url_hit+"/Notification/DeleteDlq", headers=headers, data=del_payload)
                            except Exception as e:
                                print(f"Error deleting DLQ: {e}")

                        # Cek dan kirim notifikasi jika merchant ini baru saja pulih
                        merchant_id = data.get('msgInfo', {}).get('merchantId') or data.get('msgInfo', {}).get('merchant_id')
                        check_and_send_recovery_alert(merchant_id, "virtual-account", self.internal_url_hit)
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

                else:
                    print("Key doesn't exist in JSON data", flush=True)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            print("", flush=True)

        channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        channel.start_consuming()

# Load environment variables from .env file
dotenv_path = Path('/etc/config_db/.env')
load_dotenv(dotenv_path=dotenv_path)

# Get environment variables
host_name = os.environ['host_name']
host_port = os.environ['host_port']
pika_user = os.environ['pika_user']
pika_pass = os.environ['pika_pass']
queue_name = "queue_notification_va"
max_retries = 3
stage_program = os.environ['stage_program']
internal_url_hit = os.environ['internal_url_hit']
external_url_hit = os.environ['external_url_hit']

# Number of threads
num_threads = 10  # Set the number of threads you want

# Create and start threads
threads = []
for i in range(num_threads):
    thread = RabbitMQConsumerThread(i + 1, host_name, host_port, pika_user, pika_pass, queue_name, max_retries, stage_program, internal_url_hit, external_url_hit)
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()