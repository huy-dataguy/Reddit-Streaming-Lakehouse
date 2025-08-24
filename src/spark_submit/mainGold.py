from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from transformer.goldTransformer import GoldTransformer
from transformer.silverBaseTransformer import BaseTransformer

spark = (SparkSession.builder
        .appName("SilverTransformer")
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


dfSub = BaseTransformer(spark).readData("spark_catalog.bronze.reddit_submission")
dfCmt = BaseTransformer(spark).readData("spark_catalog.bronze.reddit_comment")

gold=GoldTransformer(spark, dfSub, dfCmt)

dimTime = gold.createDimTime()
dimAuthor = gold.createDimAuthor()
dimSentiment = gold.createDimSentiment()
dimSubreddit = gold.createDimSubreddit()
dimPostType = gold.createDimPostType()
dimPost = gold.createDimPost()
dimComment = gold.createDimComment()


factPostActi = gold.createFactPostActivity(dimTime, dimAuthor, dimSubreddit, dimPostType, dimSentiment)
factCmtActi = gold. createFactCommentActivity(dimTime, dimAuthor, dimSubreddit, dimPost, dimSentiment)

gold.writeData(dimTime, "spark_catalog.gold.dimTime", checkpointPath="s3a://checkpoint/lakehouse/gold/dimTime/")
gold.writeData(dimAuthor, "spark_catalog.gold.dimAuthor", checkpointPath="s3a://checkpoint/lakehouse/gold/dimAuthor/")
gold.writeData(dimSentiment, "spark_catalog.gold.dimSentiment", checkpointPath="s3a://checkpoint/lakehouse/gold/dimSentiment/")
gold.writeData(dimSubreddit, "spark_catalog.gold.dimSubreddit", checkpointPath="s3a://checkpoint/lakehouse/gold/dimSubreddit/")
gold.writeData(dimPostType, "spark_catalog.gold.dimPostType", checkpointPath="s3a://checkpoint/lakehouse/gold/dimPostType")
gold.writeData(dimPost, "spark_catalog.gold.dimPost", checkpointPath="s3a://checkpoint/lakehouse/gold/dimPost/")
gold.writeData(dimComment, "spark_catalog.gold.dimComment", checkpointPath="s3a://checkpoint/lakehouse/gold/dimComment/")
gold.writeData(factPostActi, "spark_catalog.gold.factPostActivity", checkpointPath="s3a://checkpoint/lakehouse/gold/factPostActivity/")
gold.writeData(factCmtActi, "spark_catalog.gold.factCommentActivity", checkpointPath="s3a://checkpoint/lakehouse/gold/factCommentActivity/")

