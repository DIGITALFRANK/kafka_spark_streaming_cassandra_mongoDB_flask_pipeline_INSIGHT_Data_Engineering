from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession


def get_spark_session_instance(spark_conf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder\
            .config(conf=spark_conf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: bin/spark-submit stream_processor.py <hostname> <port> ", file=sys.stderr)
        sys.exit(-1)
    host, port = sys.argv[1:]
    sc = SparkContext(appName="UserLogStreamProcessor")
    ssc = StreamingContext(sc, .0001)

