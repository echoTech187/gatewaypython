import pika  
import json
import requests
import datetime
import pytz
import os

JakartaTz = pytz.timezone("Asia/Jakarta")

from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

host_name = os.environ['host_name']
host_port = os.environ['host_port']
pika_user = os.environ['pika_user']
pika_pass = os.environ['pika_pass']
queue_name = "queue_telegram_send_message"

stage_program = os.environ['stage_program']

internal_url_hit = os.environ['internal_url_hit']
external_url_hit = os.environ['external_url_hit']

#print("get env: %s %s %s %s" % (host_name, pika_user, pika_pass, queue_name), flush=True)

print(' [*] %s Connecting to server ... ' % datetime.datetime.now(JakartaTz), flush=True)

credentials = pika.PlainCredentials(pika_user, pika_pass)
connection = pika.BlockingConnection(pika.ConnectionParameters(host_name, host_port, '/', credentials))
channel = connection.channel()
channel.queue_declare(
    queue=queue_name, 
    durable=True, 
    arguments={
        "x-dead-letter-exchange": "dlx_notifications",
        "x-dead-letter-routing-key": queue_name + "_dlq"
    }
)

print(' [*] %s Waiting for messages.' % datetime.datetime.now(JakartaTz), flush=True)

def callback(ch, method, properties, body):  
    
    #print("Method: {}".format(method), flush=True)
    #print("Properties: {}".format(properties), flush=True)
    
    print(" [x] Received %s [xx] %s" % (datetime.datetime.now(JakartaTz), body), flush=True)
   
    data = json.loads(body)
    
    if ("msgType" in data and "msgInfo" in data):
    
        if (data['msgType'] == "consumer_telegram_send_message") :

            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            payload = json.dumps({
                "msgInfo": data['msgInfo']
            })
            
            headers = {
                'Content-Type': 'application/json'
            } 
            
            response = requests.request("POST", internal_url_hit+"/Telegram/sendMessage", headers=headers, data=payload)
            
            print(" [Get Response Code] %s [xx] %s %s" % (datetime.datetime.now(JakartaTz), response.status_code, data), flush=True)
            print(" [Get Full Response] %s [xx] %s %s" % (datetime.datetime.now(JakartaTz), response.text, data), flush=True)
            
            print("================", flush=True)

        else:
            print("Data tidak sesuai", flush=True)
        
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("Key doesn't exist in JSON data", flush=True)
    
    print("", flush=True)
    
#channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback)
channel.start_consuming()