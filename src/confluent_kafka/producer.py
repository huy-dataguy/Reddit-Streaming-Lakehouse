#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer

import json
import time

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def deliveryCallback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

def readOneLineJson(file):
    line = file.readline()
    return json.loads(line)
        

def producerRS_RC (url, topic, config):

    producer = Producer(config)
    
    cntSub = 0
    cntCom = 0
    # with open('src/data/RS_reddit.jsonl', 'r') as f:

    with open(f'{url[0]}', 'r') as fRS, open(f'{url[1]}', 'r') as fRC:

        oneSub = readOneLineJson(fRS)
        oneCom = readOneLineJson(fRC)


        while(True):
            
            if oneSub and (not oneCom or oneSub['created_utc'] <= oneCom['created_utc']):
                cntSub +=1
                producer.produce(
                    topic=topic[0],
                    key=str(cntSub),  
                    value=json.dumps(oneSub),  # convert dict -> JSON string
                    callback=deliveryCallback
                )
                producer.poll(1)
                oneSub = readOneLineJson(fRS)
            elif oneCom and (not oneSub or oneCom['created_utc'] <= oneSub['created_utc']):
                cntCom+=1
                producer.produce(
                    topic=topic[1],
                    key=str(cntCom),  
                    value=json.dumps(oneCom),  # convert dict -> JSON string
                    callback=deliveryCallback
                )
                producer.poll(1)
                oneCom = readOneLineJson(fRC)
            else:
                print("waiting for luv...")
            time.sleep(2)

    # Block until the messages are sent.
    producer.flush()


if __name__ == '__main__':
    rsrcURL = ["/home/confluent_kafka/python_kafka/data/RS_reddit.jsonl", "/home/confluent_kafka/python_kafka/data/RC_reddit.jsonl"]
    rsrcTopic = ["redditSubmission", "redditComment"]

    config = {
        'bootstrap.servers': 'kafka1:9092',
        # Fixed properties
        'acks': 'all'
    }

    producerRS_RC(rsrcURL, rsrcTopic, config)


                


