import time
import json
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer


user_logs_producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                   value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def tail_log_file(file):
    """
    - Reads all content from log file then tails file for incoming logs
    - Appends current timestamp to every log line (kafka message) to secure ingestion time
    - Use in iterator to read log content, set interval time (seconds) for desired output interval
    :param file: (open(file_path))
    """
    interval = 1.0  # checks every 1 sec for new lines if no new line is found
    while True:
        where = file.tell()
        line = file.readline()
        if not line:
            time.sleep(interval)
            file.seek(where)
        else:
            yield line


for line in tail_log_file(open('LOG_FILE_PATH')):
    user_logs_producer.send('test_topic', value=line)


