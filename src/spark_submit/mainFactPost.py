from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from transformer.goldTransformer import GoldTransformer
from utils.getSparkConfig import getSparkConfig

pathRS="spark_catalog.silver.reddit_submission"
pathRC ="spark_catalog.silver.reddit_comment"

pathDTime = "spark_catalog.gold.dimTime"
pathDAuthor = "spark_catalog.gold.dimAuthor"
pathDSubreddit = "spark_catalog.gold.dimSubreddit"
pathDPostType = "spark_catalog.gold.dimPostType"
pathDPost = "spark_catalog.gold.dimPost"
pathDComment = "spark_catalog.gold.dimComment"
pathDSentiment = "spark_catalog.gold.dimSentiment"

pathFPost = "spark_catalog.gold.factPostActivity"
pathFCmt = "spark_catalog.gold.factCmtActivity"

spark = getSparkConfig("FactPostActivity")
gold = GoldTransformer(spark)

dfSubNew=gold.readData(pathIn=pathRS, streaming=True)


def processFactPostActivity(batch_df, batch_id):
        dTime = gold.readData(pathDTime)
        dAuthor = gold.readData(pathDAuthor)
        dSubreddit = gold.readData(pathDSubreddit)
        dPostType = gold.readData(pathDPostType)
        dSentiment = gold.readData(pathDSentiment)
        dCmt = gold.readData(pathDComment)


        factPost = gold.createFactPostActivity(dTime, dAuthor, dSubreddit, dPostType, dSentiment, dCmt, batch_df)
        gold.writeData(factPost,pathOut= pathFPost)
        

qFPost = (dfSubNew.writeStream
    .option("checkpointLocation", "s3a://checkpoint/lakehouse/gold/factPostActivity/")
    .foreachBatch(processFactPostActivity)
    .start()
)

spark.streams.awaitAnyTermination()
