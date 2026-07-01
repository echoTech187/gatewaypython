import subprocess
import json
import requests
import datetime
import pytz
import os

JakartaTz = pytz.timezone("Asia/Jakarta")

from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path('/etc/config_db/.env')
load_dotenv(dotenv_path=dotenv_path)

stage_program = os.environ['stage_program']
internal_url_hit = os.environ['internal_url_hit']

def parse_nginx_log(log_line):
    try:

        log_parts = log_line.split()
        if len(log_parts) >= 21 and len(log_parts[0]) > 1:
        
            response_time = float(log_parts[9])
            http_code = log_parts[8]
            if http_code in ["200", "201", "202", "500", "501", "502", "503", "504", "505", "507", "508", "510", "511"] and response_time > 10:

                if any(substring in log_parts[5] for substring in ['/index.php?History=1']):
                    print("This is allow!")
                else:

                    print("Gidi " + stage_program + " Response Time Nginx > 10 seconds |", "IP Address:", log_parts[0], "Timestamp:", log_parts[2], "Request Method:", log_parts[4], "Server Name:", log_parts[7], "Request URL:", log_parts[5], "Status Code:", log_parts[8], "Response Time:", response_time)
                    
                    infoText = 'Gidi ' + stage_program + ' Response Time Nginx > 10 second | IP Address: ' + str(log_parts[0]) + ' | Timestamp: ' + str(log_parts[2]) + '] | Request Method: ' + str(log_parts[4]) + ' | Server Name: ' + str(log_parts[7]) + ' | Request URL: ' + str(log_parts[5]) + ' | Status Code: ' + str(log_parts[8]) + ' | Response Time: ' + str(response_time)

                    url = internal_url_hit+"/Rabbitmq/createQueue"
                    payload = json.dumps({
                        "msgType": "consumer_telegram_send_message",
                        "msgInfo": {
                            "info_text": infoText,
                            "bot_option": "webserver"
                        }
                    })
                    headers = {
                        'Content-Type': 'application/json'
                    }
                    requests.request("POST", url, headers=headers, data=payload)

    except ValueError:
        print("Failed to parse log")

log_file_path = "/var/log/nginx/access.log"

tail_command = ["tail", "-F", log_file_path]

tail_process = subprocess.Popen(tail_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

try:
    for line in tail_process.stdout:
        parse_nginx_log(line.strip())
except KeyboardInterrupt:
    pass
finally:
    tail_process.kill()