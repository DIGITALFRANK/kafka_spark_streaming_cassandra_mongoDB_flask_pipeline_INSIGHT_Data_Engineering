import time
import json
from time import sleep
from json import dumps
from kafka import KafkaProducer


users_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                               value_serializer=lambda x: json.dumps(x).encode('utf-8'))

