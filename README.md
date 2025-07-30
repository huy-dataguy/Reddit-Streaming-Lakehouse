# Reddit-GenAI-Data-Platform
Run End to End
## 1. build sparkbase image

cd Reddit-GenAI-Data-Platform/

```bash
docker build -t sparkbase -f docker/spark_on_YARN/base.dockerfile .
```

cd Reddit-GenAI-Data-Platform/docker/
## 2. build - run spark on yarn cluster

```bash
docker network create reddit_network
```
```bash
docker compose build 
```

```bash
docker compose up -d
``` 
> access master cli start dfs , start yarn

## 3. build - run minio
```bash
docker compose -f minio.compose.yaml up -d

```
## 4. build - run hive metastore

create bucket **warehouse** in minio dcm khong tao no loi

```bash
docker compose -f hive.compose.yaml up -d
```

## 5. build - run kafka cluster
```bash
docker compose -f kafka.compose.yaml up -d

```
```bash
docker exec -it kafka1 bash
```

#### 5.1 format kafka, generate the log storage

> format container kafka1, kafka2, kafka3 ok ok
```bash
./kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c ./kafka/config/kraft/server.properties
```
#### 5.2 start on all kafka cluster

> start kafka container kafka1, kafka2, kafka3 ok ok

```bash
./kafka/bin/kafka-server-start.sh ./kafka/config/kraft/server.properties
```

#### 5.3 check - show info cluster 
```bash
kafka-metadata-quorum.sh --bootstrap-controller kafka1:9093 describe --status
```
<img width="1160" height="229" alt="image" src="https://github.com/user-attachments/assets/b15217dc-4e5e-4e87-b500-b509526e5045" />

#### 5.4 create topic 
```bash
kafka-topics.sh --create --topic iceberg-topic --bootstrap-server kafka1:9092 --replication-factor 2
```
#### 5.5 describe topic
```bash
kafka-topics.sh --describe --bootstrap-server kafka1:9092 --topic iceberg-topic
```
<img width="1197" height="229" alt="image" src="https://github.com/user-attachments/assets/ce017e13-b46f-49dc-b541-e569752fc465" />

## 6. Stream data kafka to iceberg
```bash
docker exec -it confluent_kafka bash
```

#### 6.1 producer data to kafka
```bash
python python_kafka/icebergtest/producer.py
```
<img width="1148" height="247" alt="image" src="https://github.com/user-attachments/assets/7c3aa258-525d-4f47-afad-98e3c217c3b9" />


#### 6.2 consumer kafka to bronze 
```bash
docker exec -it client bash
```

- 1. access client spark

=> spark-shell
```bash
spark.sql("create database spark_catalog.bronze")
```
- 2. vim consumer.py
```python
code ở folder src/spark_submit copy paster vô <3
```
- 3. spark-submit consumer.py
```bash
spark-submit consumer.py
```
> note sau khi stream data vô thì nó lỗi dung lượng..data thì đã load được vô bronze r, nên ok, lỗi này fix sau (MN fix :))

- 4. test da tao duoc iceberg 
=> spark-shell
```bash
spark.read.table("spark_catalog.bronze.iceberg_topic").show()
```





