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
print(db.list_collection_names())

checkpointColl = db["kafka_checkpoints"]
collection = checkpointColl
for doc in collection.find():
    print(doc)



x = checkpointColl.delete_many({})

print(x.deleted_count, " documents deleted.")