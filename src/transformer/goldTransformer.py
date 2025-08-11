
from transformer.baseTransformer import BaseTransformer
from pyspark.sql import functions as F

class GoldTransformer(BaseTransformer):
    def __init__(self, sparkSession, dfSubmission, dfComment):
        super().__init__(sparkSession)
        self.dfSubmission = dfSubmission
        self.dfComment = dfComment
    
    def createDimTime(self,timestampCol="createdDate"):
        dfTimestamp=self.dfSubmission.select(timestampCol).distinct().union(self.dfComment.select(timestampCol).distinct())
        dimTime=(dfTimestamp.withColumn("year", F.year(F.col(timestampCol)))
                            .withColumn("month", F.month(F.col(timestampCol)))
                            .withColumn("day", F.dayofmonth(F.col(timestampCol)))
                            .withColumn("hour", F.hour(F.col(timestampCol)))
                            .withColumn("day_of_week", F.dayofweek(F.col(timestampCol)))
                            .withColumnRenamed(timestampCol, "time_key"))
        return dimTime
        
    def createDimAuthor(self, authorName="author", authorKey="author_fullname"):
        dimAuthor=(self.dfSubmission.select(authorName, authorKey).distinct()
            .union(self.dfComment.select(authorName, authorKey).distinct())
            .distinct()
            .withColumnRenamed(authorName, "author_name")
            .withColumnRenamed(authorKey, "author_key"))
        return dimAuthor

