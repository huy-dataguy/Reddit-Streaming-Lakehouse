import json
from pymongo import MongoClient
import certifi
import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
client = MongoClient(
    MONGODB_URI,
    tls=True,
    tlsCAFile=certifi.where()
)
db = client["reddit_db"]
collection = db["comments"]



BATCH_SIZE = 1000
batch = []

cnt=0
with open("data/RC_reddit.jsonl", "r", encoding="utf-8") as f:
    batch = []
    for line in f:
        if line.strip():
            doc = json.loads(line)
            batch.append(doc)

            if len(batch) == BATCH_SIZE:
                    collection.insert_many(batch)
                    cnt+=len(batch)
                    batch = []
if batch:
    cnt+=len(batch)
    collection.insert_many(batch)

print(f"insert {cnt} lines into mongoDB")

