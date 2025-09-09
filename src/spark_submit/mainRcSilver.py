from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from transformer.silverSubTransfomer import SubmissionTransformer
from transformer.silverCmtTransformer import CommentTransformer

from utils.getSparkConfig import getSparkConfig


spark = getSparkConfig("SilverTransformerComment")

commentTransformer = CommentTransformer(spark)

pathOut="spark_catalog.silver.reddit_comment"

dfBatch = commentTransformer.readData("spark_catalog.bronze.reddit_comment", streaming=True)

def processRC(batch_df, batch_id):
        df = commentTransformer.convertTimestamp(batch_df)
        df = commentTransformer.markAuthorDeleted(df) 
        df = commentTransformer.markBodyRemoved(df, body="body", newCol="cmtDeleted")
        df = commentTransformer.normalizeParentId(df)
        df = commentTransformer.normalizeLinkId(df)
        df = commentTransformer.markModComments(df)
        commentTransformer.writeData(df, pathOut)

query = commentTransformer.writeData(df=dfBatch, pathOut=pathOut, checkpointPath="s3a://checkpoint/lakehouse/silver/reddit_comment/", streaming=True, batchFunc=processRC)

query.awaitTermination()


