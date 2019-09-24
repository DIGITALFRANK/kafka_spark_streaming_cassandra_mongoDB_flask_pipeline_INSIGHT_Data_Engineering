# INSIGHT_Data_Engineering  

## _kafka_spark_streaming_cassandra_mongoDB_flask_real_time_data_pipeline_

# AdME
### a real time event-driven advertising platform for digital products at scale  

## Problem definition  

Advertisers need to work at the speed of data.  

For example, picture a mobile app, at scale, with millions of users. 
Users interact with the app in real-time and generate millions if not billions of data points per second. 

It is difficult for product owners and advertisers to consume and utilize this data due to its velocity.  

In addition, acting on ad targeting opportunities, or measuring the preciseness of targeting efforts 
is also a bigger challenge when user event data isn't captured in real time.

## Solution  

Working with user logs and geo-location data, 
AdME is a platform that allows product owners to track user activity & location 
in real time with a live map view and custom analytics dashboard.  

By leveraging this data in real-time, 
product owners and advertisers can derive stronger ad budgeting and targeting insights, 
as well as act more swiftly on advertising opportunities  


## Pipline Architecture  

[architecture image]  

## Kafka Ingestion: Real time multi-node cluster data ingestion and partitioning   

blah blah blah...  

## Persisting raw data in Cassandra (Distributed Persistence)  

blah blah blah...  

## Spark Streaming: Direct Approach (No Receivers)  

(from official Spark documentation)  

This new receiver-less “direct” approach has been introduced in Spark 1.3 to ensure stronger end-to-end guarantees. Instead of using receivers to receive data, this approach periodically queries Kafka for the latest offsets in each topic+partition, and accordingly defines the offset ranges to process in each batch. When the jobs to process the data are launched, Kafka’s simple consumer API is used to read the defined ranges of offsets from Kafka (similar to read files from a file system).  

This approach has the following advantages over the receiver-based approach (i.e. Approach 1).  

> **_Simplified Parallelism:_** No need to create multiple input Kafka streams and union them. With directStream, Spark Streaming will create as many RDD partitions as there are Kafka partitions to consume, which will all read data from Kafka in parallel. So there is a one-to-one mapping between Kafka and RDD partitions, which is easier to understand and tune.  

> **_Efficiency:_** Achieving zero-data loss in the first approach required the data to be stored in a Write Ahead Log, which further replicated the data. This is actually inefficient as the data effectively gets replicated twice - once by Kafka, and a second time by the Write Ahead Log. This second approach eliminates the problem as there is no receiver, and hence no need for Write Ahead Logs. As long as you have sufficient Kafka retention, messages can be recovered from Kafka.  

> **_Exactly-once semantics:_** The old approach uses Kafka’s high level API to store consumed offsets in Zookeeper. This is traditionally the way to consume data from Kafka. While this approach (in combination with write ahead logs) can ensure zero data loss (i.e. at-least once semantics), there is a small chance some records may get consumed twice under some failures. This occurs because of inconsistencies between data reliably received by Spark Streaming and offsets tracked by Zookeeper. Hence, in this second approach, we use simple Kafka API that does not use Zookeeper. Offsets are tracked by Spark Streaming within its checkpoints. This eliminates inconsistencies between Spark Streaming and Zookeeper/Kafka, and so each record is received by Spark Streaming effectively exactly once despite failures. In order to achieve exactly-once semantics for output of your results, your output operation that saves the data to an external data store must be either idempotent, or an atomic transaction that saves results and offsets.  

Note that one disadvantage of this approach is that it does not update offsets in Zookeeper, hence Zookeeper-based Kafka monitoring tools will not show progress. However, you can access the offsets processed by this approach in each batch and update Zookeeper yourself.  

## Spark Structured Streaming and data processing  

blah blah blah...  

## Front End UI 













