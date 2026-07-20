import pika
import os
import time
import datetime
import threading
import json
import requests
import pytz
import hashlib
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
dotenv_path = Path('/etc/config_db/.env')
load_dotenv(dotenv_path=dotenv_path)

host_name = os.environ.get('host_name', 'localhost')
host_port = int(os.environ.get('host_port', 5672))
pika_user = os.environ.get('pika_user', 'guest')
pika_pass = os.environ.get('pika_pass', 'guest')
internal_url_hit = os.environ['internal_url_hit']

QUEUE_NAMES = {
    "queue_notification_ewallet_dlq_final": {
        "label": "E-Wallet",
        "url_resend": "",
        "db_type": "ewallet"
    },
    "queue_notification_va_dlq_final": {
        "label": "Virtual Account",
        "url_resend": "",
        "db_type": "virtual-account"
    },
    "queue_notification_qris_mpm_dlq_final": {
        "label": "QRIS MPM",
        "url_resend": "",
        "db_type": "qris-mpm"
    },
    "queue_notification_transfer_dlq_final": {
        "label": "Transfer (Disbursement)",
        "url_resend": "",
        "db_type": "transfer"
    }
}

class DLQConsumerThread(threading.Thread):
    def __init__(self, queue_name):
        threading.Thread.__init__(self)
        self.daemon = True
        self.queue_name = queue_name

    def run(self):
        while True:
            try:
                self.consume()
            except Exception as e:
                print(f"[!] Connection lost for {self.queue_name}: {e}. Reconnecting...", flush=True)
                time.sleep(5)

    def consume(self):
        JakartaTz = pytz.timezone("Asia/Jakarta")
        print(f'[*] {datetime.datetime.now(JakartaTz)} Connecting to DLQ server for {self.queue_name}...', flush=True)

        credentials = pika.PlainCredentials(pika_user, pika_pass)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host_name, host_port, '/', credentials))
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name, durable=True)

        print(f'[*] {datetime.datetime.now(JakartaTz)} Waiting for messages in {self.queue_name}.', flush=True)

        def callback(ch, method, properties, body):
            print(f"[x] Received DLQ Message on {self.queue_name} at {datetime.datetime.now(JakartaTz)} [xx] {body}", flush=True)
            
            try:
                item = json.loads(body)
                
                # Extract type_val from queue name db_type
                type_val = QUEUE_NAMES[self.queue_name].get("db_type", "unknown")
                
                merchant_id = "UNKNOWN"
                if "msgInfo" in item and "merchantId" in item["msgInfo"]:
                    merchant_id = item["msgInfo"]["merchantId"]
                elif "msgInfo" in item and "merchant_id" in item["msgInfo"]:
                    merchant_id = item["msgInfo"]["merchant_id"]
                    
                ref_id = None
                ref_cashin = None
                ref_cashout = None
                if "msgInfo" in item:
                    for key in ['ref_cashinPaymentEwalletId', 'ref_cashinPaymentVaId', 'ref_cashinPaymentQrisMpmId']:
                        if key in item["msgInfo"]:
                            ref_id = item["msgInfo"][key]
                            ref_cashin = ref_id
                            break
                    for key in ['ref_cashoutPaymentBifastId', 'ref_cashinPaymentTransferId', 'ref_cashoutTransferBifastId', 'ref_cashoutPaymentId']:
                        if key in item["msgInfo"]:
                            ref_id = item["msgInfo"][key]
                            ref_cashout = ref_id
                            break
                            
                if merchant_id != "UNKNOWN" and ref_id:
                    failed_message = {
                        "merchantId": merchant_id,
                        "type": type_val,
                        "ref_id": ref_id,
                        "ref_cashin": ref_cashin,
                        "ref_cashout": ref_cashout,
                        "payload": body.decode('utf-8') if isinstance(body, bytes) else str(body)
                    }
                    try:
                        log_payload = json.dumps({"failed_messages": [failed_message]})
                        headers = {'Content-Type': 'application/json'}
                        res_log = requests.post(internal_url_hit+"/Notification/LogDlq", headers=headers, data=log_payload)
                        print(f"[*] Logged DLQ to DB. Status: {res_log.status_code}", flush=True)
                    except Exception as e:
                        print(f"[*] Failed to log to DLQ DB: {e}", flush=True)
            except Exception as e:
                print(f"Error parsing message on {self.queue_name}: {e}")
                
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
        channel.start_consuming()
if __name__ == "__main__":
    # Start consumer thread for each queue
    for q_name in QUEUE_NAMES.keys():
        t = DLQConsumerThread(q_name)
        t.start()
        
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...", flush=True)
