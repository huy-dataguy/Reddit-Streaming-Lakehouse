from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql.utils import AnalysisException



class BaseTransformer:
    def __init__(self, sparkSession):
        self.spark = sparkSession
    def readData(self, pathIn, format="iceberg", streaming=False):
        try:

            if streaming:
                return (self.spark
                            .readStream 
                            .format(format)
                            .load(pathIn))
            else:
                return self.spark.table(pathIn)
        except AnalysisException:
            return None
        
        
    def readSnapshot(self, pathIn, snapshots):
        #create dict
        #**options unpack dict to all key arg
        startSnap = snapshots[0]
        endSnap=snapshots[1]
        options = {}
        if startSnap is not None:
            options["start-snapshot-id"] = str(startSnap)
        if endSnap is not None:
            options["end-snapshot-id"] = str(endSnap)
        return (self.spark.read
                    .format("iceberg")
                    .options(**options)
                    .load(pathIn))


    def writeData(self, df, pathOut, format="iceberg", checkpointPath=None, mode="append", streaming=False, batchFunc=None):
        if streaming:
            query = (df.writeStream
            # query = (df.writeStream
                .format(format)
                .outputMode(mode)
                .trigger(processingTime="5 seconds")
                .option("checkpointLocation", checkpointPath)
                .foreachBatch(batchFunc)
                .start())
            return query
            # query.awaitTermination()  
    
        else:
            (df.write
                .format(format)
                .mode(mode)
                .saveAsTable(pathOut))
            return None


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