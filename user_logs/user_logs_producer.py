import time
import json
from datetime import datetime
from time import sleep
from json import dumps
from kafka import KafkaProducer


class UserLogsProducer:
    example_json_log = {
        "log": {
            "source": "",
            "type": "",
            "datetime": "",
            "log_user_id": "",
            "log": ""
        }
    }

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
    def produce_msgs(self, source_symbol):
        pass


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

    # bullshit logs / delete asap
    log_1 = {
        "log": {
            "source": "1",
            "type": "user_log",
            "datetime": datetime.utcnow(),
            "log_user_id": "12",
            "log": "1"
        }
    }
    log_2 = {
        "log": {
            "source": "2",
            "type": "user_log",
            "datetime": datetime.utcnow(),
            "log_user_id": "2",
            "log": "1"
        }
    }
    log_3 = {
        "log": {
            "source": "1",
            "type": "ad_view_log",
            "datetime": datetime.utcnow(),
            "log_user_id": "120",
            "log": "1"
        }
    }
    log_4 = {
        "log": {
            "source": "1",
            "type": "user_log",
            "datetime": datetime.utcnow(),
            "log_user_id": "110",
            "log": "1"
        }
    }
    log_5 = {
        "log": {
            "source": "8",
            "type": "ad_view_log",
            "datetime": datetime.utcnow(),
            "log_user_id": "112",
            "log": "1"
        }
    }

    test_logs = [log_1, log_2, log_3, log_4, log_5]

    for log in test_logs:
        producer.send_log("test_topic_2", log)


if __name__ == "__main__":
    main()
