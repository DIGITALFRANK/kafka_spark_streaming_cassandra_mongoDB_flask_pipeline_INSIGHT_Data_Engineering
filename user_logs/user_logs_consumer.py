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


# according to documentation, this may drop messages that haven't yet arrived to the queue
for message in user_logs_consumer:
    timestamp = datetime.utcnow()  # adds current timestamp to every kafka message at consumption
    value = message.value
    atom = json.load({"timestamp": timestamp, "value": value})

    # Spark Structured Streaming stuff here
    # What's your windowing?
    # How are we appending to DataFrame?
    # Sort after each window append?


# Consuming messages in this fashion ensures no data loss, even if messages have yet to arrive to queue
while True:
    raw_messages = user_logs_consumer.poll(timeout_ms=1000, max_records=5000)
    for topic_partition, message in raw_messages.items():
        application_message = json.loads(message.value.decode())
