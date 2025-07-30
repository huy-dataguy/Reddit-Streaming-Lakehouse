from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, IntegerType, StringType

spark = (SparkSession.builder
    .appName("Kafka to Iceberg Bronze")
    .enableHiveSupport()
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083")
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://warehouse/iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "mypassword")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate())

schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("fan", StringType())

df_kafka = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:9092")
    .option("subscribe", "iceberg-topic")
    .option("startingOffsets", "earliest")
    .load())

df_parsed = (df_kafka
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*"))

query = (df_parsed.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://warehouse/iceberg/bronze/checkpoints/iceberg_topic/")
    .toTable("spark_catalog.bronze.iceberg_topic"))

query.awaitTermination()
