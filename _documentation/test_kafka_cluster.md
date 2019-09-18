
# 1
**CREATE KAFKA TOPIC FROM MASTER NODE**

$ /usr/local/kafka/bin/kafka-topics.sh --create \
    --zookeeper localhost:2181 \
    --replication-factor 3 \
    --partitions 4 \
    --topic test_topic 


# 2
**DESCRIBE TOPIC FROM ANOTHER NODE (CHECK THAT CLUSTER IS WORKING EFFECTIVELY)**

$ /usr/local/kafka/bin/kafka-topics.sh --describe \
    --zookeeper localhost:2181 \
    --topic test_topic


# 3
**USE THE CONSOLE PRODUCER TO TEST PRODUCE SOME DATA (from a third node)**

$ /usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

**ENTER SOME DATA INTO THE PRODUCER**
> this is a message

# 4
**USE THE CONSOLE CONSUMER TO TEST CONSUME SOME DATA (from a forth node)**

$ /usr/local/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic test_topic

**IF YOU ACHIEVE ALL THESE STEPS, YOUR KAFKA CLUSTER IS PROPERLY CONFIGURED, 
YOU CAN FURTHER TEST FOR FAULT TOLERANCE BY 
KILLING A `leader` NODE AND CHECKING HOW KAFKA PICKS UP**

