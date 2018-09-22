import config
from time import sleep
from json import dumps
from kafka import KafkaProducer

# Setting broker details in the bootstrap_servers array and serializing data
producer = KafkaProducer(bootstrap_servers=[config.broker],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Sending serializer data from producer to topic 'testing'
for y in range(config.iterations):
    data = {'number': y}
    producer.send(config.topic, data)
    sleep(config.time)