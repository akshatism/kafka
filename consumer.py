from pymongo import MongoClient
from kafka import KafkaConsumer
from json import loads

# Setting client for MongoDB and creating a collection for writing the data
client = MongoClient('localhost:27017')
collection = client.testing.testing

# Setting consumer details and deserializing data from utf-8 encoding to json
consumer = KafkaConsumer('testing',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id="0",
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

# Consumer taking data from topic iteratively and writing it to MongoDB collection we specified above
for msg in consumer:
    msg = msg.value
    collection.insert_one(msg)
    print('{} added to {}'.format(msg,collection))