#!/usr/bin/env python3
import sys
import json
from time import sleep
import threading
from datetime import datetime
import random
import uuid
from fluent import sender, event
import socket


hostname = socket.gethostname()
IPAddr = socket.gethostbyname(hostname)
port=1269


fluentd_logger = sender.FluentSender('Payment', host='localhost', port=24224)

NODE_ID = 1

def registration():
    registration_log = {
        "node_id": NODE_ID,
        "message_type": "REGISTRATION",
        "service_name": "Payment service",
        "log_level": "REGISTRATION",
        "ipaddr": str(IPAddr),
        "port": str(port),
        "timestamp": datetime.now().isoformat()
    }
    return registration_log

def generate_log():
    random_log_type = random.randint(1, 100)
    if random_log_type <= 60:  
        info_log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "INFO",
            "message_type": "LOG",
            "message": "some comment",
            "service_name": "Payment service",
            "timestamp": datetime.now().isoformat()
        }
        return info_log

    elif (random_log_type >= 61 and random_log_type <= 90):  
        warn_log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "WARN",
            "message_type": "LOG",
            "message": "warning comment",
            "service_name": "Payment service",
            "response_time_ms": "update",
            "threshold_limit_ms": "update",
            "timestamp": datetime.now().isoformat()
        }
        return warn_log

    else:  
        error_log = {
            "log_id": str(uuid.uuid4()),
            "node_id": NODE_ID,
            "log_level": "ERROR",
            "message_type": "LOG",
            "message": "some message",
            "service_name": "Payment service",
            "error_details": {"error_code": "some code", "error_message": "some message"},
            "timestamp": datetime.now().isoformat()
        }
        return error_log

topic = "logs"
heartbeat_interval = 5
interval = 1

def heartbeat():
    while True:
        heartbeat_log = {
            "node_id": NODE_ID,
            "message_type": "HEARTBEAT",
            "service_name": "Payment service",
            "log_level": "HEARTBEAT",
            "timestamp": datetime.now().isoformat()
        }
        fluentd_logger.emit(topic, heartbeat_log)
        # print(heartbeat_log)
        sleep(heartbeat_interval)

heartbeat_thread = threading.Thread(target=heartbeat)
heartbeat_thread.daemon = True 
heartbeat_thread.start()

try:
    registrationmsg = registration()
    fluentd_logger.emit(topic, registrationmsg)
    # print(registrationmsg)

    while True:
        log = generate_log()
        fluentd_logger.emit(topic, log)
        #print(log)
        sleep(interval)

except KeyboardInterrupt:
    print("Stopping node.")
