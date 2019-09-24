import time
import json
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer


class UserLogsProducer:
    def __init__(self, kafka_brokers):
        """
        Producer constructor, serializes json data
        :param kafka_brokers: List
        """
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_log(self, topic, json_log):
        """
        Sends single log to kafka topic as encoded json
        Requires source, use on iterable to dynamically populate kafka topic
        :param json_log: Json
        :param topic: Str
        """
        self.producer.send(topic, key=b'user_log', value=json_log)

    # when simulating data, we will use produce_msgs instead of send_log since we won't have a source
    # (see 'kafka advanced' in DE ecosystem)
    def produce_msgs(self):
        for line in tail_log_file(open('../data/user_logs.log')):
            self.producer.send('test_topic', value=line)


# helper function to tail a log file, we might just 86 this and dynamically generate data in various ways
# or use it, who knows!
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

# example use case with kafka producer
# for line in tail_log_file(open('LOG_FILE_PATH')):
#     user_logs_producer.send('test_topic', value=line)


def main():
    producer = UserLogsProducer(['localhost:9092'])
    producer.produce_msgs()


if __name__ == "__main__":
    main()
