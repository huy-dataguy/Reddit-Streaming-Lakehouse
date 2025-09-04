from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from transformer.silverSubTransfomer import SubmissionTransformer
from transformer.silverCmtTransformer import CommentTransformer

spark = (SparkSession.builder
        .appName("SilverTransformerSubmission")
        .enableHiveSupport()
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://checkpoint/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio1:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "mypassword")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate())


submissionTransformer = SubmissionTransformer(spark)
submissionTransformer.transform("spark_catalog.bronze.reddit_submission", "spark_catalog.silver.reddit_submission", checkpointPath="s3a://checkpoint/lakehouse/silver/reddit_submission/", streaming=True)

