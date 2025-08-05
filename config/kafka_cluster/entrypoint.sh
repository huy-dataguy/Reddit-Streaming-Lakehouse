#!/bin/bash

set -e
#format
if [ ! -f ./kafka/kraft-combined-logs/meta.properties ]; then
  ./kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c ./kafka/config/kraft/server.properties
fi
#run
./kafka/bin/kafka-server-start.sh ./kafka/config/kraft/server.properties

