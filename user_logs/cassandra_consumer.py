import sys
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster

consumer = KafkaConsumer(
    'test_topic_2',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

cluster = Cluster(['spark-cluster'])
session = cluster.connect('fitbit')

# start the loop
try:
    for message in consumer:
        entry = json.loads(json.loads(message.value))['log']
        print("Entry: {} Source: {} Type: {}".format(
            entry['datetime'],
            entry['source'],
            entry['type']))
        print("Log: {}".format(entry['log']))
        print("--------------------------------------------------")
        session.execute(
            """
            INSERT INTO user_logs (log_source, log_type, log_datetime, log_user_id, log, log_id)
            VALUES (%s, %s, %s, %s, %s, now())
            """,
            (entry['source'],
             entry['type'],
             entry['datetime'],
             entry['log_user_id'],
             entry['log']))

except KeyboardInterrupt:
    sys.exit()
