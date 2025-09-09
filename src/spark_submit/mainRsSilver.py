from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from transformer.silverSubTransfomer import SubmissionTransformer
from transformer.silverCmtTransformer import CommentTransformer
from utils.getSparkConfig import getSparkConfig


spark = getSparkConfig("SilverTransformerSubmission")


submissionTransformer = SubmissionTransformer(spark)
pathOut = "spark_catalog.silver.reddit_submission"

dfBatch = submissionTransformer.transform("spark_catalog.bronze.reddit_submission", streaming=True)


def processRS(batch_df, batch_id):
        df = submissionTransformer.convertTimestamp(batch_df)
        df = submissionTransformer.markAuthorDeleted(df) 
        df = submissionTransformer.markBodyRemoved(df)
        df = submissionTransformer.markSpamPost(df)
        submissionTransformer.writeData(df, pathOut)
    
query = submissionTransformer.writeData(df=dfBatch, pathOut=pathOut, checkpointPath="s3a://checkpoint/lakehouse/silver/reddit_submission/", streaming=True, batchFunc=processRS)

query.awaitTermination()
