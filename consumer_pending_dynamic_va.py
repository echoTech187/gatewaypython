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
                if data['msgType'] == "consumer_pending_dynamic_va":
                    payload = json.dumps({
                        "msgInfo": data['msgInfo']
                    })

                    headers = {
                        'Content-Type': 'application/json'
                    }

                    try:


                        response = requests.request("POST", internal_url_hit+"/Pending/dynamicVa", headers=headers, data=payload, timeout=30)


                        print(f"[Get Response Code] {datetime.datetime.now(JakartaTz)} [xx] {response.status_code} [xx] {data} (Thread {self.thread_id})", flush=True)


                        print(f"[Get Full Response] {datetime.datetime.now(JakartaTz)} [xx] {response.text} [xx] {data} (Thread {self.thread_id})", flush=True)


                        print("================ (Thread {self.thread_id})", flush=True)



                        if response.status_code == 200:


                            ch.basic_ack(delivery_tag=method.delivery_tag)


                        else:


                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


                            print(f"[!] {datetime.datetime.now(JakartaTz)} Rejected, moved to DLQ (Thread {self.thread_id})", flush=True)


                    except Exception as e:


                        print(f"[!] {datetime.datetime.now(JakartaTz)} Exception: {e}. Moved to DLQ (Thread {self.thread_id})", flush=True)


                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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
queue_name = "queue_pending_dynamic_va"
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