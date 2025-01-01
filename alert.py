#!/usr/bin/env python3
from kafka import KafkaConsumer
import json
import sys
import time
import threading

topics = ["INFO", "ERROR", "REGISTRATION", "WARN", "HEARTBEAT"]
bs = '192.168.20.128:9092'
regis = []
reg = []
last_heartbeat = {}  
heartbeat_timeout = 7 
heartbeat_check_interval = 5  

consumer = KafkaConsumer(
    *topics,
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    bootstrap_servers=bs  
)

print("Listening to Kafka topics:", topics)


def check_heartbeats():
    global regis
    while True:
        current_time = time.time()  
        for node_id, last_time in list(last_heartbeat.items()):
            if current_time - last_time > heartbeat_timeout:
                print(f"Missed heartbeat for Node {node_id}, please check!")
                print(f"message_type: DEREGISTRY\nnode_id: {node_id}\nstatus: DOWN\ntimestamp: {current_time}\n")
                regis.remove(node_id)  
                del last_heartbeat[node_id]  
        time.sleep(heartbeat_check_interval)  

heartbeat_thread = threading.Thread(target=check_heartbeats, daemon=True)
heartbeat_thread.start()

try:
    for message in consumer:
        reg = regis.copy()
        obj = message.value
        current_time = time.time() 

        if obj.get("message_type") == "REGISTRATION":
            nid = obj.get("node_id", "N/A")
            snm = obj.get("service_name", "N/A")
            tsp = obj.get("timestamp", "N/A")
            regis.append(nid)
            last_heartbeat[nid] = current_time  
            print(f"message_type: REGISTRY\nnode_id: {nid}\nservice_name: {snm}\nstatus: UP\ntimestamp: {tsp}\n")

        elif obj.get("message_type") == "HEARTBEAT":
            nid = obj.get("node_id")
            if nid is not None and int(nid) in reg:
                reg.remove(int(nid))
            
            if nid is not None:
                last_heartbeat[nid] = current_time

        elif obj.get("log_level") == "ERROR":
            nid = obj.get("node_id", "N/A")
            snm = obj.get("service_name", "N/A")
            ed = obj.get("error_details", "NA")
            erc = ed.get("error_code", "N/A")
            erm = ed.get("error_message", "N/A")
            print(f"log_level: ERROR\nnode_id: {nid}\nservice_name: {snm}\nerror_code: {erc}\nerror_message: {erm}\n")

        elif obj.get("log_level") == "WARN":
            nid = obj.get("node_id", "N/A")
            snm = obj.get("service_name", "N/A")
            msg = obj.get("message", "N/A")
            print(f"log_level: WARN\nnode_id: {nid}\nservice_name: {snm}\nmessage: {msg}\n")

        sys.stdout.flush()

except KeyboardInterrupt:
    print("\nShutting down consumer...")
finally:
    consumer.close()
