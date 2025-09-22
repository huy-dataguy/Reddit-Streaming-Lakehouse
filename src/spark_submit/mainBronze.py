from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from utils.getSparkConfig import getSparkConfig


spark = getSparkConfig("Stream Sub, Cmt to Iceberg Bronze")

rSubmissionBronzeSchema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("url", StringType(), True),
    StructField("permalink", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("post_hint", StringType(), True),
    StructField("author", StringType(), True),
    StructField("author_fullname", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("subreddit", StringType(), True),
    StructField("subreddit_id", StringType(), True),
    StructField("subreddit_name_prefixed", StringType(), True),
    StructField("subreddit_type", StringType(), True),
    StructField("subreddit_subscribers", IntegerType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("total_awards_received", IntegerType(), True),
    StructField("edited", BooleanType(), True),
    StructField("locked", BooleanType(), True),
    StructField("spoiler", BooleanType(), True),
    StructField("over_18", BooleanType(), True),
    StructField("stickied", BooleanType(), True),   
    StructField("retrieved_on", LongType(), True),
    StructField("is_original_content", BooleanType(), True),
    StructField("link_flair_text", StringType(), True)
])



rCommentBronzeSchema = StructType([
    StructField("id", StringType(), True),
    StructField("body", StringType(), True),
    StructField("created_utc", LongType(), True),
    StructField("edited", BooleanType(), True),
    StructField("score", IntegerType(), True),
    StructField("author", StringType(), True),
    StructField("author_fullname", StringType(), True),
    StructField("author_created_utc", LongType(), True),
    StructField("parent_id", StringType(), True),
    StructField("link_id", StringType(), True),
    StructField("is_submitter", BooleanType(), True),
    StructField("permalink", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("subreddit_id", StringType(), True),
    StructField("subreddit_name_prefixed", StringType(), True),
    StructField("subreddit_type", StringType(), True),
    StructField("total_awards_received", IntegerType(), True),
    StructField("controversiality", IntegerType(), True),
    StructField("retrieved_on", LongType(), True),
    StructField("stickied", BooleanType(), True)
])


dfSubs = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:9092")
    .option("subscribe", "redditSubmission")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10)
    .load())


dfParsedSub = (dfSubs.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), rSubmissionBronzeSchema).alias("data"))
    .select("data.*"))

querySub = (dfParsedSub.writeStream 
    .format("iceberg") 
    .outputMode("append") 
    .option("checkpointLocation", "s3a://checkpoint/lakehouse/bronze/reddit_submission/") 
    .toTable("spark_catalog.bronze.reddit_submission")  
    )


dfCom = (spark.readStream 
    .format("kafka") 
    .option("kafka.bootstrap.servers", "kafka1:9092") 
    .option("subscribe", "redditComment") 
    .option("startingOffsets", "earliest") 
    .option("maxOffsetsPerTrigger", 20)  
    .load())

dfParsedCom = (dfCom.selectExpr("CAST(value AS STRING) as json_str") 
    .select(from_json(col("json_str"), rCommentBronzeSchema).alias("data")) 
    .select("data.*"))

queryCom = (dfParsedCom.writeStream 
    .format("iceberg") 
    .outputMode("append") 
    .option("checkpointLocation", "s3a://checkpoint/lakehouse/bronze/reddit_comment/") 
    .toTable("spark_catalog.bronze.reddit_comment"))

spark.streams.awaitAnyTermination()