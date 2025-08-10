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
docker compose -f spark.compose.yml build
```

```bash
docker compose -f spark.compose.yml up -d
``` 
> access master cli start dfs , start yarn

## 3. build - run minio
```bash
docker compose -f minio.compose.yaml up -d

```

## 4. build - run hive metastore


```bash
docker compose -f hive.compose.yaml up -d
```

## 5. build - run kafka cluster
```bash
docker compose -f kafka.compose.yaml build
```

```bash
docker compose -f kafka.compose.yaml up -d
```

#### 1. check - show info cluster 

```bash
docker exec -it kafka1 bash
```
```bash
kafka-topics.sh --create --topic testbitnami --bootstrap-server kafka1:9092 --replication-factor 2
kafka-topics.sh --create --topic redditSubmission --bootstrap-server kafka1:9092 --replication-factor 2
kafka-topics.sh --create --topic redditComment --bootstrap-server kafka1:9092 --replication-factor 2
```
```bash
kafka-console-consumer.sh --topic redditSubmission --from-beginning --bootstrap-server kafka1:9092
```

```bash
kafka-metadata-quorum.sh --bootstrap-controller kafka1:9093 describe --status
```
<img width="1160" height="229" alt="image" src="https://github.com/user-attachments/assets/b15217dc-4e5e-4e87-b500-b509526e5045" />


#### 2. list topic
```bash
  kafka-topics.sh --bootstrap-server kafka1:9092 --list
```

## 6. Stream data kafka to iceberg
```bash
docker exec -it confluent_kafka bash
```

#### 6.1 producer data to kafka
```bash
python scripts/producer.py
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
spark.sql("create database spark_catalog.silver")
spark.sql("create database spark_catalog.gold")

```
<img width="626" height="104" alt="image" src="https://github.com/user-attachments/assets/55f21c7b-3430-4165-a032-fda0cec3c08c" />

- 3. stream kafka
```bash
spark-submit spark_submit/rsBronze.py
```

```bash
spark-submit spark_submit/rcBronze.py
```
<img width="792" height="129" alt="image" src="https://github.com/user-attachments/assets/759db55e-8fe9-402c-bb9d-5b28d2a628a8" />

- 4. transform silver
  - a. chuyen folder transformer thanh file transformer.zip
```bash
  spark-submit --py-files transformer.zip main.py
```

     
```bash
spark-shell \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

```bash
spark.read.table("spark_catalog.bronze.reddit_comment").show()
```

```bash
spark.read.table("spark_catalog.bronze.reddit_submission").show()
```




mc rm -r --force minio1/checkpoint/bronze/
