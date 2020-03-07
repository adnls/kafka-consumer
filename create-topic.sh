#!/bin/bash

~/kafka/kafka_2.12-2.4.0/bin/kafka-topics.sh \
      --create \
      --bootstrap-server localhost:9092 \
      --replication-factor $3 \
      --partitions $2 \
      --topic $1

~/kafka/kafka_2.12-2.4.0/bin/kafka-topics.sh \
      --list \
      --bootstrap-server localhost:9092
