#!/usr/bin/env python

import time
import json
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'kafka1:9092',
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'failed: {err}')
    else:
        print(f'to {msg.topic()} [{msg.partition()}]')

data = {
    "id": 1,
    "name": "huy",
    "fan": "hallo"
}

try:
    while True:
        json_data = json.dumps(data)
        producer.produce('testbitnami', value=json_data, callback=delivery_report)
        producer.flush()
        time.sleep(5)
except KeyboardInterrupt:
    print("coook")
