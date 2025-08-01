from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

spark = (SparkSession.builder
    .appName("Stream Comments to Iceberg Bronze")
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

rCommentBronzeSchema = StructType([
    StructField("all_awardings", ArrayType(StructType([
        StructField("award_type", StringType()),
        StructField("coin_price", IntegerType()),
        StructField("coin_reward", IntegerType()),
        StructField("count", IntegerType()),
        StructField("days_of_drip_extension", IntegerType()),
        StructField("days_of_premium", IntegerType()),
        StructField("description", StringType()),
        StructField("icon_height", IntegerType()),
        StructField("icon_url", StringType()),
        StructField("icon_width", IntegerType()),
        StructField("id", StringType()),
        StructField("is_enabled", BooleanType()),
        StructField("name", StringType()),
        StructField("resized_icons", ArrayType(StructType([
            StructField("height", IntegerType()),
            StructField("url", StringType()),
            StructField("width", IntegerType())
        ]))),
        StructField("subreddit_id", StringType(), True)
    ]))),
    
    StructField("author", StringType()),
    StructField("author_created_utc", LongType()),
    StructField("author_flair_background_color", StringType(), True),
    StructField("author_flair_css_class", StringType(), True),
    StructField("author_flair_richtext", ArrayType(StringType())),  # ***file data null
    StructField("author_flair_template_id", StringType(), True),
    StructField("author_flair_text", StringType(), True),
    StructField("author_flair_text_color", StringType(), True),
    StructField("author_flair_type", StringType()),
    StructField("author_fullname", StringType()),
    StructField("author_patreon_flair", BooleanType()),
    
    StructField("body", StringType()),
    StructField("can_gild", BooleanType()),
    StructField("can_mod_post", BooleanType()),
    StructField("collapsed", BooleanType()),
    StructField("collapsed_reason", StringType(), True),
    StructField("controversiality", IntegerType()),
    StructField("created_utc", LongType()),
    StructField("distinguished", StringType(), True),
    StructField("edited", BooleanType()),  # ***
    StructField("gilded", IntegerType()),
    StructField("gildings", MapType(StringType(), IntegerType())),
    
    StructField("id", StringType()),
    StructField("is_submitter", BooleanType()),
    StructField("link_id", StringType()),
    StructField("locked", BooleanType()),
    StructField("no_follow", BooleanType()),
    StructField("parent_id", StringType()),
    StructField("permalink", StringType()),
    StructField("quarantined", BooleanType()),
    StructField("removal_reason", StringType(), True),
    StructField("retrieved_on", LongType()),
    StructField("score", IntegerType()),
    StructField("send_replies", BooleanType()),
    StructField("stickied", BooleanType()),
    
    StructField("subreddit", StringType()),
    StructField("subreddit_id", StringType()),
    StructField("subreddit_name_prefixed", StringType()),
    StructField("subreddit_type", StringType()),
    StructField("total_awards_received", IntegerType())
])

#.option("endingOffsets", "latest")

dfCom = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092") \
    .option("subscribe", "redditComment") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 5)  \
    .load()

dfParsed = dfCom.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), rCommentBronzeSchema).alias("data")) \
    .select("data.*")

query = dfParsed.writeStream \
    .format("iceberg") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .toTable("spark_catalog.bronze.reddit_comment")

#    .option("checkpointLocation", "s3a://warehouse/bronze/checkpoints/reddit_comment/") \


query.awaitTermination()