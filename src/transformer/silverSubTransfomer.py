from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from transformer.silverBaseTransformer import BaseTransformer
    

class SubmissionTransformer(BaseTransformer):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)
 
    
    def markSpamPost(self, df, titleCol="title", urlCol="url", domainCol="domain", subredditCol="subreddit", newCol="isPostSpam"):
        words = F.split(F.lower(F.col(titleCol)), " ")
        numMatched = F.size(
            F.expr(f"filter(split(lower({titleCol}), ' '), word -> instr(lower({urlCol}), word) > 0)")
        )
        totalWord = F.size(words)
        matchPercent = F.when(totalWord == 0, F.lit(0)).otherwise((numMatched * 100.0) / totalWord)

        isPostSpam = (matchPercent >= 60) & (
            F.col(domainCol) != F.concat(F.lit("self."), F.col(subredditCol))
        )

        return df.withColumn("matchPercent", matchPercent).withColumn(newCol, isPostSpam)

         
    def transform(self, pathIn, format="iceberg", streaming=False):
        df = self.readData(pathIn, format, streaming=streaming)
        df = self.convertTimestamp(df)
        df = self.markAuthorDeleted(df) 
        df = self.markBodyRemoved(df)
        df = self.markSpamPost(df)
        return df
