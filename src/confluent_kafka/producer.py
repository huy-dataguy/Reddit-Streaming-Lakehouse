#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer
import json
import time
if __name__ == '__main__':

    config = {
        'bootstrap.servers': 'kafka1:9092',

        # Fixed properties
        'acks': 'all'
    }

    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    topic = "demo"
    
    cnt = 0
    # with open('src/data/RS_reddit.jsonl', 'r') as f:
    with open('/home/confluent_kafka/python_kafka/data/RS_reddit.jsonl', 'r') as f:
        for line in f:
            cnt+=1
            data = json.loads(line)
            producer.produce(
                topic=topic,
                key=str(cnt),  
                value=json.dumps(data),  # convert dict -> JSON string
                callback=delivery_callback
            )
            producer.poll(1)
            time.sleep(5)

    # Block until the messages are sent.
    producer.flush()