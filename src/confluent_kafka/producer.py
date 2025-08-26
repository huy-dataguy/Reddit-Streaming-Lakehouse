#!/usr/bin/env python

from confluent_kafka import Producer
from pymongo import MongoClient
from bson import ObjectId
import json
import time
import os
import certifi
from dotenv import load_dotenv


load_dotenv("/home/confluent_kafka_user/scripts/.env")
# connect mongodb 
MONGODB_URI = os.getenv("MONGODB_URI")
client = MongoClient(
    MONGODB_URI,
    tls=True,
    tlsCAFile=certifi.where()
)
mongoDB = client["reddit_db"]
postColl = mongoDB["posts"]
cmtColl = mongoDB["comments"]

checkpointColl = "kafka_checkpoints"

#check each successful produce to ensure no data loss on restart
def getLastCheckpoint(mongoDB, checkpointColl, source):
    checkpoint = mongoDB[checkpointColl].find_one(
        {"source": source}, sort=[("_id", -1)]
    )
    if checkpoint:
        return checkpoint.get("last_id"), checkpoint.get("cnt", 0)
    return None, 0


# save status, save the location of the record loaded into kafka

def saveCheckpoint(mongoDB, checkpointColl, last_id, source, cnt):
    mongoDB[checkpointColl].insert_one({
        "source": source,
        "last_id": last_id,
        "cnt": cnt,
        "timestamp": time.time()
    })


def deliveryCallback(err, msg):
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(f"Produced event to topic {msg.topic()}: key={msg.key().decode()}")

#read one record
def readRecord(cursor):
    try:
        return next(cursor)
    except StopIteration:
        return None



#sending each post, comment to its respective Kafka topic.
def producerRS_RC(postColl, cmtColl, rsrcTopic, config, checkpointColl, mongoDB):

    producer = Producer(config)

    # checkpoint cho posts
    lastIdPost, cntSub = getLastCheckpoint(mongoDB, checkpointColl, "posts")


    
    queryPost = {"_id": {"$gt": ObjectId(lastIdPost)}} if lastIdPost else {}
    cursorPost = postColl.find(queryPost).sort("_id", 1)

    # checkpoint cho comments
    lastIdCmt, cntCom = getLastCheckpoint(mongoDB, checkpointColl, "comments")

    queryCmt = {"_id": {"$gt": ObjectId(lastIdCmt)}} if lastIdCmt else {}
    cursorComment = cmtColl.find(queryCmt).sort("_id", 1)

    oneSub = readRecord(cursorPost)
    oneCom = readRecord(cursorComment)

    while True:
        if oneSub and (not oneCom or oneSub["created_utc"] <= oneCom["created_utc"]):
            cntSub += 1
            producer.produce(
                topic=rsrcTopic[0],
                key=str(cntSub),
                value=json.dumps(oneSub, default=str),
                callback=deliveryCallback
            )
            saveCheckpoint(mongoDB, checkpointColl, str(oneSub["_id"]), "posts", cntSub)

            producer.poll(1)
            oneSub = readRecord(cursorPost)

        elif oneCom and (not oneSub or oneCom["created_utc"] <= oneSub["created_utc"]):
            cntCom += 1
            producer.produce(
                topic=rsrcTopic[1],
                key=str(cntCom),
                value=json.dumps(oneCom, default=str),
                callback=deliveryCallback
            )
            saveCheckpoint(mongoDB, checkpointColl, str(oneCom["_id"]), "comments", cntCom)

            producer.poll(1)
            oneCom = readRecord(cursorComment)

        else:
            print("No new posts/comments... waiting...")
        # time.sleep(2)

    producer.flush()


if __name__ == '__main__':
    rsrcTopic = ["redditSubmission", "redditComment"]

    config = {
        "bootstrap.servers": "kafka1:9092",
        "acks": "all"
    }

    producerRS_RC(postColl, cmtColl, rsrcTopic, config, checkpointColl, mongoDB)
