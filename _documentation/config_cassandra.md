# 1
**CREATE CASSANDRA CLUSTER**  

# 2
**CREATE KEYSPACE WITH REPLICATION FACTOR 3**  

$ bin/cqlsh cass1.fitbit.lab  
Connected to AdminTome Cluster at cass1.admintome.lab:9042.  
[cqlsh 5.0.1 | Cassandra 3.11.3 | CQL spec 3.4.4 | Native protocol v4]  
Use HELP for help.  
cqlsh>  


cqlsh> CREATE KEYSPACE fitbit WITH replication =  \
{'class': 'SimpleStrategy', 'replication_factor': '3'}  \
AND durable_writes = true;

# 3

**SELECT YOUR KEYSPACE (database)**

cqlsh> USE fitbit;  

**CREATE USER_LOG TABLE**


cqlsh> CREATE TABLE fitbit.user_logs (  \
log_id timeuuid,  \
log_user_id integer,  \
log_source text,  \
log_type text,  \
log text,  \
log_datetime text,  \
PRIMARY KEY ((log_source, log_type, log_user_id), log_id)  \
) WITH CLUSTERING ORDER BY (log_id DESC)  