from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from transformer.goldTransformer import GoldTransformer
from transformer.silverBaseTransformer import BaseTransformer
from utils.getIdSnapshot import getIdSnapshot
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

#dfSub get new data from 2 snapshot ID - in the first get all new data from first snapshot
pathRS="spark_catalog.silver.reddit_submission"
# snapshotsSub=getIdSnapshot(spark, pathRS)

#### convert to readStream dont need to use read Snapshot start-end...........
#******************
# dfSubNew = BaseTransformer(spark).readSnapshot(pathRS, snapshotsSub)

pathRC ="spark_catalog.silver.reddit_comment"
# snapshotsCmt=getIdSnapshot(spark, pathRC)
# dfCmtNew = BaseTransformer(spark).readSnapshot(pathRC, snapshotsCmt)

dfSubNew = BaseTransformer(spark).readData(pathRS, streaming=True)
dfCmtNew = BaseTransformer(spark).readData(pathRC, streaming=True)

                                            
gold=GoldTransformer(spark, dfSubNew, dfCmtNew)

# func get lastest id snapshot
# get existingDim time from snapshot
pathDTime = "spark_catalog.gold.dimTime"
# snapshotTime = getIdSnapshot(spark, pathDTime)
# existDimTime = BaseTransformer(spark).readSnapshot(pathDTime, snapshotTime)

existDimTime = BaseTransformer(spark).readData(pathDTime, streaming=True)
dimTime = gold.createDimTime(existingDimTime=existDimTime) 
gold.writeData(dimTime, pathDTime, checkpointPath="s3a://checkpoint/lakehouse/gold/dimTime/", streaming=True)

# Get existing dimAuthor from snapshot for stream/batch processing
pathDAuthor = "spark_catalog.gold.dimAuthor"
# snapshotAuthor = getIdSnapshot(spark, pathDAuthor)
# existDimAuthor = BaseTransformer(spark).readSnapshot(pathDAuthor, snapshotAuthor)
existDimAuthor = BaseTransformer(spark).readData(pathDAuthor, streaming=True)

dimAuthor = gold.createDimAuthor(existingAuthor=existDimAuthor)
gold.writeData(dimAuthor, pathDAuthor, checkpointPath="s3a://checkpoint/lakehouse/gold/dimAuthor/", streaming=True)


# Only create dimSentiment if it does not exist yet
pathDSentiment = "spark_catalog.gold.dimSentiment"
# snapshotSentiment = getIdSnapshot(spark, pathDSentiment)
# existDimSentiment = BaseTransformer(spark).readSnapshot(pathDSentiment, snapshotSentiment)

existDimSentiment = BaseTransformer(spark).readData(pathDSentiment)
if existDimSentiment is None or existDimSentiment.rdd.isEmpty():
        dimSentiment = gold.createDimSentiment()
        gold.writeData(dimSentiment, pathDSentiment, checkpointPath="s3a://checkpoint/lakehouse/gold/dimSentiment/")
else:
        dimSentiment=existDimSentiment

# Get existing dimSubreddit from snapshot for stream/batch processing
pathDSubreddit = "spark_catalog.gold.dimSubreddit"
# snapshotSubreddit = getIdSnapshot(spark, pathDSubreddit)
# existDimSubreddit = BaseTransformer(spark).readSnapshot(pathDSubreddit, snapshotSubreddit)
existDimSubreddit = BaseTransformer(spark).readData(pathDSubreddit, streaming=True)

dimSubreddit = gold.createDimSubreddit(existingSubreddit=existDimSubreddit)
gold.writeData(dimSubreddit, pathDSubreddit, checkpointPath="s3a://checkpoint/lakehouse/gold/dimSubreddit/", streaming=True)


pathDPostType = "spark_catalog.gold.dimPostType"
# snapshotPostType = getIdSnapshot(spark, pathDPostType)
# existDimPostType = BaseTransformer(spark).readSnapshot(pathDPostType, snapshotPostType)
existDimPostType =BaseTransformer(spark).readData(pathDPostType, streaming=True)
dimPostType = gold.createDimPostType(existingPostType=existDimPostType)
gold.writeData(dimPostType, pathDPostType, checkpointPath="s3a://checkpoint/lakehouse/gold/dimPostType", streaming=True)


dfSub = BaseTransformer(spark).readData(pathRS)
pathDPost = "spark_catalog.gold.dimPost"
dimPost = gold.createDimPost(dfSub)
gold.writeData(dimPost, pathDPost, mode="overwrite" ,checkpointPath="s3a://checkpoint/lakehouse/gold/dimPost/")


pathDComment = "spark_catalog.gold.dimComment"
# snapshotComment = getIdSnapshot(spark, pathDComment)
# existDimComment = BaseTransformer(spark).readSnapshot(pathDComment, snapshotComment)
existDimComment=BaseTransformer(spark).readData(pathDComment, streaming=True)
dimComment = gold.createDimComment(existingCmt=existDimComment)
gold.writeData(dimComment,pathDComment, checkpointPath="s3a://checkpoint/lakehouse/gold/dimComment/", streaming=True)


factPostActi = gold.createFactPostActivity(dimTime, dimAuthor, dimSubreddit, dimPostType, dimSentiment)
factCmtActi = gold. createFactCommentActivity(dimTime, dimAuthor, dimSubreddit, dimPost, dimSentiment)

gold.writeData(factPostActi, "spark_catalog.gold.factPostActivity", checkpointPath="s3a://checkpoint/lakehouse/gold/factPostActivity/", streaming=True)
gold.writeData(factCmtActi, "spark_catalog.gold.factCommentActivity", checkpointPath="s3a://checkpoint/lakehouse/gold/factCommentActivity/", streaming= True)

