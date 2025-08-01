from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json


spark = (SparkSession.builder
    .appName("Stream Submissions to Iceberg Bronze")
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

    # .config("spark.hadoop.fs.s3a.fast.upload", "true") 
    # .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") 
    # .config("spark.hadoop.fs.s3a.multipart.size", "10485760") 
    # .config("spark.hadoop.fs.s3a.multipart.threshold", "10485760") 
    # .config("spark.hadoop.fs.s3a.connection.maximum", "50") 
    
   


    .getOrCreate())


rSubmissionBronzeSchema = StructType([
    StructField("all_awardings", ArrayType(StructType([
        StructField("award_type", StringType(), True),
        StructField("coin_price", IntegerType(), True),
        StructField("coin_reward", IntegerType(), True),
        StructField("count", IntegerType(), True),
        StructField("days_of_drip_extension", IntegerType(), True),
        StructField("days_of_premium", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("icon_height", IntegerType(), True),
        StructField("icon_url", StringType(), True),
        StructField("icon_width", IntegerType(), True),
        StructField("id", StringType(), True),
        StructField("is_enabled", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("resized_icons", ArrayType(StructType([
            StructField("height", IntegerType(), True),
            StructField("url", StringType(), True),
            StructField("width", IntegerType(), True)
        ]))),
        StructField("subreddit_id", StringType())
    ]))),
    StructField("archived", BooleanType(), True),
    StructField("author", StringType(), True),
    StructField("author_created_utc", LongType(), True),
    StructField("author_flair_background_color", StringType(), True),
    StructField("author_flair_css_class", StringType(), True),
    StructField("author_flair_richtext", ArrayType(StringType()), True),  # **file data null nulll
    StructField("author_flair_template_id", StringType(), True),
    StructField("author_flair_text", StringType(), True),
    StructField("author_flair_text_color", StringType(), True),
    StructField("author_flair_type", StringType(), True),
    StructField("author_fullname", StringType(), True),
    StructField("author_patreon_flair", BooleanType(), True),
    StructField("can_gild", BooleanType(), True),
    StructField("can_mod_post", BooleanType(), True),
    StructField("category", StringType(), True),
    StructField("content_categories", StringType(), True),
    StructField("contest_mode", BooleanType(), True),
    StructField("created_utc", LongType() , True),
    StructField("distinguished", StringType() , True),
    StructField("domain", StringType() , True),
    StructField("edited", BooleanType(), True),
    StructField("gilded", IntegerType() , True),
    StructField("gildings", MapType(StringType(), IntegerType())),
    StructField("hidden", BooleanType()),
    StructField("id", StringType()),
    StructField("is_crosspostable", BooleanType()),
    StructField("is_meta", BooleanType()),
    StructField("is_original_content", BooleanType()),
    StructField("is_reddit_media_domain", BooleanType()),
    StructField("is_robot_indexable", BooleanType()),
    StructField("is_self", BooleanType()),
    StructField("is_video", BooleanType()),
    StructField("link_flair_background_color", StringType()),
    StructField("link_flair_css_class", StringType()),
    StructField("link_flair_richtext", ArrayType(StringType())),
    StructField("link_flair_text", StringType()),
    StructField("link_flair_text_color", StringType()),
    StructField("link_flair_type", StringType()),
    StructField("locked", BooleanType()),
    StructField("media", StringType()),
    StructField("media_embed", MapType(StringType(), StringType())),
    StructField("media_only", BooleanType()),
    StructField("no_follow", BooleanType()),
    StructField("num_comments", IntegerType()),
    StructField("num_crossposts", IntegerType()),
    StructField("over_18", BooleanType()),
    StructField("parent_whitelist_status", StringType()),
    StructField("permalink", StringType()),
    StructField("pinned", BooleanType()),
    StructField("pwls", IntegerType()),
    StructField("quarantine", BooleanType()),
    StructField("removal_reason", StringType()),
    StructField("retrieved_on", LongType()),
    StructField("score", IntegerType()),
    StructField("secure_media", StringType()),
    StructField("secure_media_embed", MapType(StringType(), StringType())),
    StructField("selftext", StringType()),
    StructField("send_replies", BooleanType()),
    StructField("spoiler", BooleanType()),
    StructField("stickied", BooleanType()),
    StructField("subreddit", StringType()),
    StructField("subreddit_id", StringType()),
    StructField("subreddit_name_prefixed", StringType()),
    StructField("subreddit_subscribers", IntegerType()),
    StructField("subreddit_type", StringType()),
    StructField("suggested_sort", StringType()),
    StructField("thumbnail", StringType()),
    StructField("thumbnail_height", IntegerType()),
    StructField("thumbnail_width", IntegerType()),
    StructField("title", StringType()),
    StructField("total_awards_received", IntegerType()),
    StructField("url", StringType()),
    StructField("whitelist_status", StringType()),
    StructField("wls", IntegerType())
])

# .option("startingOffsets", "latest")

# dfSubs = (spark.readStream
#     .format("kafka")
#     .option("kafka.bootstrap.servers", "kafka1:9092")
#     .option("subscribe", "redditSubmission")
#     .option("startingOffsets", "earliest")
#     .option("maxOffsetsPerTrigger", 1)
#     .load())
dfSubs = (spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:9092")
    .option("subscribe", "redditSubmission")
    .option("startingOffsets", "earliest")
    .load())

dfParsed = (dfSubs.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), rSubmissionBronzeSchema).alias("data"))
    .select("data.*"))

# query = (dfParsed.writeStream 
#     .format("iceberg") 
#     .trigger(processingTime="30 seconds") 
#     .outputMode("append") 
#     .option("checkpointLocation", "s3a://warehouse/bronze/checkpoints/reddit_submission/") 
#     .toTable("spark_catalog.bronze.reddit_submission")  
#     )

query = (dfParsed.write
    .format("iceberg")
    .mode("append")
    .option("checkpointLocation", "s3a://warehouse/bronze/checkpoints/reddit_submission/")
    .saveAsTable("spark_catalog.bronze.reddit_submission")
)
#debguggggggggggggg
# query = (dfParsed.writeStream 
#     .format("console") 
#     .outputMode("append") 
#     .option("truncate", False) 
#     .start())


query.awaitTermination()