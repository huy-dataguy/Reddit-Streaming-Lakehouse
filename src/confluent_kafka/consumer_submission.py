#!/usr/bin/env python

from confluent_kafka import Consumer
from multiprocessing import Process
import time
import json


def consumerInstance(consumerID, topic):
    config = {
        'bootstrap.servers': 'kafka1:9092',

        'group.id':          'GRredditSubmission',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)

    # Subscribe to topic
    consumer.subscribe([topic]) #register consumer to topic

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print(f"Consumer {consumerID} Waiting...")
            elif msg.error():
                print(f"Consumer {consumerID} ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print("Consumer {consumerID} Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(consumerID = consumerID,
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

if __name__ == '__main__':
    numConsumer = 6
    processes = []
    topic = "redditSubmission"
    for i in range(numConsumer):
        p = Process(target=consumerInstance, args=(i, topic))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()