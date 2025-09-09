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

querys=[]

pathRS="spark_catalog.silver.reddit_submission"
pathRC ="spark_catalog.silver.reddit_comment"
pathDTime = "spark_catalog.gold.dimTime"
pathDAuthor = "spark_catalog.gold.dimAuthor"
pathDSentiment = "spark_catalog.gold.dimSentiment"
pathDSubreddit = "spark_catalog.gold.dimSubreddit"
pathDPost = "spark_catalog.gold.dimPost"
pathDComment = "spark_catalog.gold.dimComment"
pathDPostType = "spark_catalog.gold.dimPostType"


gold = GoldTransformer(spark, pathRS, pathRC, True)
                                            
existDimTime =gold.readData(pathDTime, streaming=True)
def processDimTime(batch_df, batch_id):
        dimTime = gold.createDimTime(existingDimTime=batch_df) 
        gold.writeData(dimTime, pathDTime)

qDTime=gold.writeData(df=existDimTime, pathOut=pathDTime, checkpointPath="s3a://checkpoint/lakehouse/gold/dimTime/", streaming=True, batchFunc=processDimTime)
querys.append(qDTime)

existDimAuthor = gold.readData(pathDAuthor, streaming=True)
def processDimAuthor(batch_df, batch_id):
        dimTime = gold.createDimAuthor(existingAuthor=batch_df) 
        gold.writeData(dimTime, pathDAuthor)

qDAuthor=gold.writeData(df=existDimAuthor, pathOut=pathDAuthor, checkpointPath="s3a://checkpoint/lakehouse/gold/dimAuthor/", streaming=True, batchFunc=processDimAuthor)
querys.append(qDAuthor)

existDimSentiment = gold.readData(pathDSentiment)
if existDimSentiment is None:
        dimSentiment = gold.createDimSentiment()
        gold.writeData(dimSentiment, pathDSentiment)
else:
        dimSentiment=existDimSentiment

existDimSubreddit = gold.readData(pathDSubreddit, streaming=True)
def processDimSubreddit(batch_df, batch_id):
        dimSubreddit = gold.createDimSubreddit(existingSubreddit=batch_df)
        gold.writeData(dimSubreddit, pathDSubreddit)
qDSubreddit=gold.writeData(existDimSubreddit, pathOut=pathDSubreddit,checkpointPath="s3a://checkpoint/lakehouse/gold/dimSubreddit/", streaming=True, batchFunc=processDimSubreddit)
querys.append(qDSubreddit)

existDimPostType =gold.readData(pathDPostType, streaming=True)
def processDPostType(batch_df, batch_id):
        dimPostType = gold.createDimPostType(existingPostType=batch_df)
        gold.writeData(dimPostType, pathDPostType)
qDPostType=gold.writeData(existDimPostType, pathDPostType, checkpointPath="s3a://checkpoint/lakehouse/gold/dimPostType", streaming=True, batchFunc=processDPostType)
querys.append(qDPostType)


dfSub=gold.readData(pathRS)
dfCmtNew=gold.dfCmtNew
def processDPost(batch_df, batch_id):
        dimPost = gold.createDimPost(dfOldSub=batch_df)
        gold.writeData(dimPost, pathRS, mode="override")

qDPost=gold.writeData(df=dfCmtNew, pathOut=pathRC,checkpointPath="s3a://checkpoint/lakehouse/gold/dimPost/", streaming=True, batchFunc=processDPost)
querys.append(qDPost)

existDimComment=gold.readData(pathDComment, streaming=True)
def processDCmt(batch_df, batch_id):
        dimComment = gold.createDimComment(existingCmt=batch_df)
        gold.writeData(dimComment,pathDComment)
qDCmt=gold.writeData(existDimComment, pathDComment, checkpointPath="s3a://checkpoint/lakehouse/gold/dimComment/", streaming=True, batchFunc=processDCmt)
querys.append(qDCmt)

def processFactPostActivity(batch_df, batch_id):
        dTime = gold.readData(pathDTime)
        dAuthor = gold.readData(pathDAuthor)
        dSubreddit = gold.readData(pathDSubreddit)
        dPostType = gold.readData(pathDPostType)
        dSentiment = gold.readData(pathDSentiment)

        factPost = gold.createFactPostActivity(dTime, dAuthor, dSubreddit, dPostType, dSentiment)
        factPost=gold.writeData(factPost,pathOut= pathFPost)

dfSubNew=gold.dfSubNew
qFPost =gold.writeData(dfSubNew, pathOut= pathFPost, checkpointPath="s3a://checkpoint/lakehouse/gold/factPostActivity/", streaming=True, batchFunc=processFactPostActivity)
querys.append(pathFPost)


def processFactCommentActivity(batch_df, batch_id):
        dTime = gold.readData(pathDTime)
        dAuthor = gold.readData(pathDAuthor)
        dSubreddit = gold.readData(pathDSubreddit)
        dPost= gold.readData(pathDPost)
        dSentiment = gold.readData(pathDSentiment)

        factComment = gold.createFactCommentActivity(dTime, dAuthor, dSubreddit, dPost, dSentiment)
        factComment=gold.writeData(factComment, pathOut=pathFCmt)


qFCmt =gold.writeData(dfCmtNew, pathOut= pathFCmt, checkpointPath="s3a://checkpoint/lakehouse/gold/factCommentActivity/", streaming=True, batchFunc=processFactCommentActivity)
querys.append(pathFCmt)

spark.streams.awaitAnyTermination()


dfCmtNew=gold.dfCmtNew

qDCmt=gold.writeData(dfCmtNew, batchFunc=processDCmt)
qDPost=gold.writeData(dfCmtNew, batchFunc=processDPost)