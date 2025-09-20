#!/bin/bash

# 1. build sparkbase image for spark on yarn
echo "Building sparkbase image"
docker build -t sparkbase -f docker/spark_on_YARN/base.dockerfile .
echo "sparkbase image built successfully"

cd docker

# 2. create network
echo "Creating Docker network 'reddit_network'"
docker network create reddit_network

# 3. build image for all service
docker compose -f spark.compose.yml build &
docker compose -f kafka.compose.yaml build &
docker compose -f trino.compose.yaml build &
docker compose -f superset.compose.yml build &
curl -Lf 'https://airflow.apache.org/docs/apache-airflow/3.0.6/docker-compose.yaml' -o airflow.compose.yaml


wait

source ../scripts/editAirflow.sh
# 4. start container
echo "starting all services..."
docker compose -f spark.compose.yml up -d &
docker compose -f minio.compose.yaml up -d &    
docker compose -f hive.compose.yaml up -d &
docker compose -f kafka.compose.yaml up -d &
docker compose -f superset.compose.yml up -d &
docker compose -f trino.compose.yaml up -d &
docker compose -f airflow.compose.yaml up -d

wait


# Append rsa_id rsa_pub to airflow
docker exec -u airflow docker-airflow-worker bash -c "mkdir -p ~/.ssh"
docker cp ../config/.ssh/. docker-airflow-worker:/home/airflow/.ssh/

# docker exec -u airflow docker-airflow-worker bash -c "mkdir ~/.ssh"
# docker exec -u airflow docker-airflow-worker bash -c "touch ~/.ssh/id_rsa"
# docker exec -u airflow docker-airflow-worker bash -c "touch ~/.ssh/id_rsa.pub"

# docker cp ../config/.ssh/id_rsa docker-airflow-worker:/tmp/id_rsa
# docker exec -u airflow airflow-worker bash -c "cat /tmp/id_rsa >> ~/.ssh/id_rsa"

# docker cp ../config/.ssh/id_rsa.pub docker-airflow-worker:/tmp/id_rsa.pub
# docker exec -u airflow airflow-worker bash -c "cat /tmp/id_rsa.pub >> ~/.ssh/id_rsa.pub"

docker exec -u sparkuser master bash -lc "start-dfs.sh"
docker exec -u sparkuser master bash -lc "start-yarn.sh"
sleep 20

echo "All services have been started successfully"

# 5. create topics
echo "Create topics kafka"
echo "Waiting Kafka broker to be ready..."
sleep 20
docker exec -it kafka1 bash -c "su kafka_user -c '~/kafka/bin/kafka-topics.sh --create --topic redditSubmission --bootstrap-server kafka1:9092 --replication-factor 2 --partitions 6'"
docker exec -it kafka1 bash -c "su kafka_user -c '~/kafka/bin/kafka-topics.sh --create --topic redditComment --bootstrap-server kafka1:9092 --replication-factor 2 --partitions 6'"
# 6
cd ..

cd config/superset
source ./setup_superset.sh
