#!/bin/bash -ex

/home/joshua/Downloads/kafka_*/bin/kafka-topics.sh --delete --topic myTopic --zookeeper localhost:2181
sleep 5
/home/joshua/Downloads/kafka_*/bin/kafka-topics.sh --create --topic myTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
