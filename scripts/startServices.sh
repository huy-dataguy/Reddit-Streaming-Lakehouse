#!/bin/bash

# --- 1. Build sparkbase image ---
echo "--- Building sparkbase image... ---"
docker build -t sparkbase -f docker/spark_on_YARN/base.dockerfile .

if [ $? -ne 0 ]; then
  echo "Error: Failed to build sparkbase image. Exiting."
  exit 1
fi
echo "sparkbase image built successfully."

# --- 2. Build and run Spark on YARN cluster ---
echo "--- Building and running Spark on YARN cluster... ---"
cd docker/

# Kiểm tra và tạo network nếu chưa tồn tại
if ! docker network ls | grep -q reddit_network; then
  echo "Creating Docker network 'reddit_network'..."
  docker network create reddit_network
fi

docker compose -f spark.compose.yml build
if [ $? -ne 0 ]; then
  echo "Error: Failed to build Spark images. Exiting."
  exit 1
fi

docker compose -f spark.compose.yml up -d
if [ $? -ne 0 ]; then
  echo "Error: Failed to start Spark containers. Exiting."
  exit 1
fi
echo "Spark on YARN cluster is running."

# --- 3. Build and run MinIO ---
echo "--- Building and running MinIO... ---"
docker compose -f minio.compose.yaml up -d
if [ $? -ne 0 ]; then
  echo "Error: Failed to start MinIO container. Exiting."
  exit 1
fi
echo "MinIO is running."

# --- 4. Build and run Hive Metastore ---
echo "--- Building and running Hive Metastore... ---"
docker compose -f hive.compose.yaml up -d
if [ $? -ne 0 ]; then
  echo "Error: Failed to start Hive Metastore container. Exiting."
  exit 1
fi
echo "Hive Metastore is running."

# --- 5. Build and run Kafka cluster ---
echo "--- Building and running Kafka cluster... ---"
docker compose -f kafka.compose.yaml build
if [ $? -ne 0 ]; then
  echo "Error: Failed to build Kafka images. Exiting."
  exit 1
fi

docker compose -f kafka.compose.yaml up -d
if [ $? -ne 0 ]; then
  echo "Error: Failed to start Kafka containers. Exiting."
  exit 1
fi
echo "Kafka cluster is running."

echo "--- All services have been started successfully! ---"