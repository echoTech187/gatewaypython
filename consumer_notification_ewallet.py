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
        channel.queue_declare(
            queue=self.queue_name, 
            durable=True, 
            arguments={
                "x-dead-letter-exchange": "dlx_notifications",
                "x-dead-letter-routing-key": self.queue_name + "_dlq"
            }
        )

        print(f'[*] {datetime.datetime.now(JakartaTz)} Waiting for messages. (Thread {self.thread_id})', flush=True)

        def callback(ch, method, properties, body):
            print(f"[x] Received {datetime.datetime.now(JakartaTz)} [xx] {body} (Thread {self.thread_id})", flush=True)

            data = json.loads(body)

            if "msgType" in data and "msgInfo" in data:
                if data['msgType'] == "consumer_notification_ewallet":
                    payload = json.dumps({
                        "msgInfo": data['msgInfo']
                    })

                    headers = {
                        'Content-Type': 'application/json'
                    }

                    try:


                        response = requests.request("POST", internal_url_hit+"/Notification/Ewallet", headers=headers, data=payload, timeout=30)


                        print(f"[Get Response Code] {datetime.datetime.now(JakartaTz)} [xx] {response.status_code} [xx] {data} (Thread {self.thread_id})", flush=True)


                        print(f"[Get Full Response] {datetime.datetime.now(JakartaTz)} [xx] {response.text} [xx] {data} (Thread {self.thread_id})", flush=True)


                        print(f"================ (Thread {self.thread_id})", flush=True)



                        if response.status_code == 200:
                            is_success = False
                            try:
                                resp_json = response.json()
                                if resp_json.get("responseCode") == "SUCCESS":
                                    is_success = True
                            except ValueError:
                                pass

                            if not is_success:
                                error_msg = f"HTTP 200 but Not SUCCESS. Response: {response.text}"
                                props = pika.BasicProperties(delivery_mode=2, headers={"x-error-detail": error_msg})
                                ch.basic_publish(exchange="dlx_notifications", routing_key=self.queue_name + "_dlq", body=body, properties=props)
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                                print(f"[!] {datetime.datetime.now(JakartaTz)} Rejected (Not SUCCESS), moved to DLQ manually (Thread {self.thread_id})", flush=True)
                            else:
                                ch.basic_ack(delivery_tag=method.delivery_tag)
                                print(f"[-] {datetime.datetime.now(JakartaTz)} Processed successfully (HTTP {response.status_code}) (Thread {self.thread_id})", flush=True)

                    except Exception as e:
                        error_msg = f"System Exception/Timeout: {str(e)}"
                        props = pika.BasicProperties(delivery_mode=2, headers={"x-error-detail": error_msg})
                        ch.basic_publish(exchange="dlx_notifications", routing_key=self.queue_name + "_dlq", body=body, properties=props)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        print(f"[!] {datetime.datetime.now(JakartaTz)} Exception: {e}. Moved to DLQ manually (Thread {self.thread_id})", flush=True)

                else:
                    print("Key doesn't exist in JSON data", flush=True)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

            print("", flush=True)

        channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        channel.start_consuming()

# Load environment variables from .env file
load_dotenv()

# Get environment variables
host_name = os.environ['host_name']
host_port = os.environ['host_port']
pika_user = os.environ['pika_user']
pika_pass = os.environ['pika_pass']
queue_name = "queue_notification_ewallet"
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