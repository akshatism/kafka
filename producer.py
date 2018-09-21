from time import sleep
from json import dumps
from kafka import KafkaProducer

# Setting broker details in the bootstrap_servers array and serializing data
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Sending serializer data from producer to topic 'testing'
for x in range(20):
    data = {'number' : x}
    producer.send('testing',data)
    sleep(1)