#!/bin/bash
./kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c ./kafka/config/kraft/server.properties
./kafka/bin/kafka-server-start.sh ./kafka/config/kraft/server.properties
