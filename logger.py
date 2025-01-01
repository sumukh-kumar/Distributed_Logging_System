#!/usr/bin/env python3
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import sys

topics = ["INFO", "ERROR", "REGISTRATION", "WARN", "HEARTBEAT"]

consumer = KafkaConsumer(
    *topics,
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    bootstrap_servers='192.168.20.128:9092'  
)

client = Elasticsearch("http://localhost:9200")  
db = "database1"

if not client.indices.exists(index=db):
    client.indices.create(index=db)


for message in consumer: 
    try:
        client.index(index=db, document=message.value)  
        print(f"Logged {message.value} into elasticsearch")
    except Exception as e:
        print(f"Error {e}")
        print(message.value)