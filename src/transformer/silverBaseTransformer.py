from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

class BaseTransformer:
    def __init__(self, sparkSession):
        self.spark = sparkSession
    def readData(self, pathIn, format="iceberg"):
        return self.spark.table(pathIn)

    def writeData(self, df, pathOut, format="iceberg", checkpointPath=None, mode="append", streaming=False):
        ## theem khi read stream, read batch airflow
        (df.write
        .format(format)
        .mode(mode)
        .saveAsTable(pathOut))

    def showShape(self, df):
        return (len(df.columns), df.count())

    def convertTimestamp(self, df, colName="created_utc", newCol="createdDate"):
        return df.withColumn(
            newCol,
            F.from_unixtime(F.col(colName)).cast("timestamp")
        )

    def markAuthorDeleted(self, df, authorCol="author", nameAuthor="author_fullname", newCol="accDeleted"):
        return df.withColumn(
            newCol,
            (F.col(authorCol) == "[deleted]") & F.col(nameAuthor).isNull()
        )

    def markBodyRemoved(self, df, body="selftext", newCol="postDeleted"):
        return df.withColumn(
            newCol,
            F.col(body) == "[removed]"
        )
    
    def transform(self):
        raise NotImplementedError("This method should be overridden by subclasses")