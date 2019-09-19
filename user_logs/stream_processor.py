# from pyspark.streaming.kafka import KafkaUtils
#
#
#
#
#
# # create direct kafka stream
# directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
#
# # output stream to kafka topic
# directKafkaStream.writeStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
#     .option("topic", "updates") \
#     .start()


# I need this to go to a dataframe, test that it's streaming: df.isStreaming()
# maybe not in stream_processor if rerwite to Kafka topic, but definitly for Spark => Mongo
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

if __name__ == "__main__":
    sc = SparkContext(appName="TestReceiveKafkaStream")
    ssc = StreamingContext(sc, 2)  # 2 second window
    broker, topic = sys.argv[1:]  # first to arguments after spark-submit stuff
    kvs = KafkaUtils.createStream(ssc,
                                  broker,
                                  "raw-event-streaming-consumer",
                                  {topic: 1})

    # do what you need in Spark, example below:

    # lines = kvs.map(lambda x: x[1])
    # counts = lines.flatMap(lambda line: line.split(" "))
    #               .map(lambda word: (word, 1)) \
    #               .reduceByKey(lambda a, b: a+b)
    # counts.pprint()

    ssc.start()
    ssc.awaitTermination()