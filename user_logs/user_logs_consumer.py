import json
from kafka import KafkaConsumer


user_logs_consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))


for message in user_logs_consumer:
    message = message.value
    print(message)
