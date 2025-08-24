from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from transformer.baseTransformer import BaseTransformer
    

class SubmissionTransformer(BaseTransformer):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)

    def markSpamPost(self, df, titleCol="title", urlCol="url", domainCol="domain", subredditCol="subreddit", newCol="isPostSpam"):
        words = F.split(F.lower(F.col(titleCol)), " ")
        numMatched = F.size(
        F.expr(f"filter(split(lower({titleCol}), ' '), word -> instr(lower({urlCol}), word) > 0)")
        )        
        totalWord = F.size(words)
        matchPercent = (numMatched * 100) / totalWord
        isPostSpam = (matchPercent >= 0.6) & (
        F.col(domainCol) != F.concat(F.lit("self."), F.col(subredditCol))
        )
        return df.withColumn("matchPercent", matchPercent).withColumn(newCol, isPostSpam)   
         
    def transform(self, pathIn, pathOut, format="iceberg", checkpointPath=None, mode="append", streaming=False):
        df = self.readData(pathIn, format)
        df = self.convertTimestamp(df)
        df = self.markAuthorDeleted(df) 
        df = self.markBodyRemoved(df)
        df = self.markSpamPost(df)
        self.writeData(df=df, pathOut=pathOut)