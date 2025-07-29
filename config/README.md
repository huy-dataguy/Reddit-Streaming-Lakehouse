config kafka, spark, trino, superset......


> **NOTE** 

wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar -O iceberg.jar


# Insert to .bashrc
KAFKA_CLUSTER_ID="Q_6ATv-PTJGaFkf27OW8Bg"

# delete log 
rm -rf /kafka/kraft-combined-logs

# generate the log storage
./kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c ./kafka/config/kraft/server.properties

# start on all machine cluster
./kafka/bin/kafka-server-start.sh ./kafka/config/kraft/server.properties

# show info cluster 
kafka-metadata-quorum.sh --bootstrap-controller kafka1:9093 describe --status

# create topic 
kafka-topics.sh --create --topic first-topic --bootstrap-server kafka1:9092 --replication-factor 2

# describe topic
kafka-topics.sh --describe --bootstrap-server kafka1:9092 --topic first-topic

# produce messing
kafka-console-producer.sh --topic first-topic --bootstrap-server kafka1:9092

# consume messing
kafka-console-consumer.sh --topic first-topic --from-beginning --bootstrap-server kafka1:9092


kafka-console-consumer.sh   --topic jsontokafka   --bootstrap-server kafka1:9092   --from-beginning   --property print.key=true


# describe group consumer
## list
kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --list
reddit_submission
## detail group consumer
kafka-consumer-groups.sh --bootstrap-server kafka1:9092   --describe --group redditSubmission









# docker cp docker-zookeeper-1:/home/zookeeper-u/zookeeper ../config/zookeeper
# docker cp docker-kafka-1:/home/kafka-u/kafka ../config/kafka

#start kafka connect zookeep run in the background, CANT use CLI
# ./kafka/bin/kafka-server-start.sh ./kafka/config/server.properties

# ./kafka/bin/kafka-server-start.sh ./kafka/config/kraft/server.properties


#create topic 
#./kafka/bin/kafka-topics.sh --create --topic test-topic --bootstrap-server kafka:9092 --replication-factor 1 --partitions 64

#describe topic
#./kafka/bin/kafka-topics.sh --topic test-topic --bootstrap-server kafka:9092 --describe

#run producer
# ./kafka/bin/kafka-console-producer.sh --topic test-topic --bootstrap-server kafka:9092

#run consumer
# ./kafka/bin/kafka-console-consumer.sh --topic test-topic --bootstrap-server kafka:9092

# ./kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test-topic --from-beginning

# the way to check zookeeper actual support kafka in coordinator metadata, write, read management
# open zookeeper CLI
# ls / # it will show all arg
# ls /brokers/ids # show all id of all brokers in kafka




####KRAFT


#kafka-u@kafka1:~/kafka$ ./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
