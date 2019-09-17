import json
from datetime import datetime
from kafka import KafkaConsumer


user_logs_consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))


# adds current timestamp to every kafka message at consumption
for message in user_logs_consumer:
    timestamp = datetime.utcnow()
    value = message.value
    atom = json.load({"timestamp": timestamp, "value": value})

    # Spark Structured Streaming stuff here
    # What's your windowing?
    # How are we appending to DataFrame?
    # Sort after each window append?