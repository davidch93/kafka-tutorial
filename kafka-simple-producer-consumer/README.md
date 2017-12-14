# Simple Producer Consumer
Kafka simple Producer & Consumer tutorial.

## Basic Knowledge
1. [Kafka Failover Cluster](http://cloudurable.com/blog/kafka-tutorial-kafka-failover-kafka-cluster/index.html)

## Before You Start
1. Copy server properties file
   ```bash
   $ cp kafka/config/server.properties kafka/lab2/config/server-0.properties
   $ cp kafka/config/server.properties kafka/lab2/config/server-1.properties
   $ cp kafka/config/server.properties kafka/lab2/config/server-2.properties
   ```
   Change properties `broker.id`, `port`, `log.dirs` with different value.
2. Start server
   ```bash
   $ kafka/bin/zookeeper-server-start.sh config/zookeeper.properties
   $ kafka/bin/kafka-server-start.sh config/server.properties
   ```
3. Create topic
   ```bash
   $ kafka/bin/kafka-topics.sh --create \
   $       --replication-factor 3 \
   $       --partitions 13 \
   $       --topic my-failover-topic \
   $       --zookeeper  localhost:2181
   ```
4. List created topics
   ```bash
   $ kafka/bin/kafka-topics.sh --list \
   $       --zookeeper localhost:2181
   ```
