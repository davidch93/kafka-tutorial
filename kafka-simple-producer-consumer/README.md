# Simple Producer Consumer
Kafka simple Producer & Consumer tutorial.

## Basic Knowledge
Source http://cloudurable.com/blog/kafka-tutorial-kafka-failover-kafka-cluster/index.html

## Before You Start
```Bash
## Create topics
kafka/bin/kafka-topics.sh --create \
    --replication-factor 3 \
    --partitions 13 \
    --topic my-failover-topic \
    --zookeeper  localhost:2181


## List created topics
kafka/bin/kafka-topics.sh --list \
    --zookeeper localhost:2181
```
