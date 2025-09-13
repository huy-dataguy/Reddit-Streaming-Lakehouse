from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from transformer.goldTransformer import GoldTransformer
from transformer.silverBaseTransformer import BaseTransformer
from utils.getIdSnapshot import getIdSnapshot
from utils.getSparkConfig import getSparkConfig


spark = getSparkConfig("GoldTransformDim")


pathRS="spark_catalog.silver.reddit_submission"
pathRC ="spark_catalog.silver.reddit_comment"

pathDTime = "spark_catalog.gold.dimTime"
pathDAuthor = "spark_catalog.gold.dimAuthor"
pathDSubreddit = "spark_catalog.gold.dimSubreddit"
pathDPostType = "spark_catalog.gold.dimPostType"

gold = GoldTransformer(spark)
dfSubNew=gold.readData(pathIn=pathRS, streaming=True)
dfCmtNew=gold.readData(pathIn=pathRC, streaming=True)
  

def processSubNew(batch_df, batch_id):
        # existDimTime = gold.readData(pathDTime, streaming=False)
        # dimTime = gold.createDimTime(existingDimTime=existDimTime, dfNew=batch_df) 
        # gold.writeData(dimTime, pathDTime)
        # existDimAuthor = gold.readData(pathDAuthor, streaming=False)
        # dimAuthor = gold.createDimAuthor(existingAuthor=existDimAuthor, dfNew=batch_df) 
        # gold.writeData(dimAuthor, pathDAuthor)

        # existDimSubreddit = gold.readData(pathDSubreddit)
        # dimSubreddit = gold.createDimSubreddit(existingSubreddit=existDimSubreddit, dfNew=batch_df)
        # gold.writeData(dimSubreddit, pathDSubreddit)
        existDimPostType =gold.readData(pathDPostType)
        dimPostType = gold.createDimPostType(existingPostType=existDimPostType, dfNew=batch_df)
        gold.writeData(dimPostType, pathDPostType)


qSubNew=(dfSubNew.writeStream
                .option("checkpointLocation","s3a://checkpoint/lakehouse/gold/subNewww/")
                .foreachBatch(processSubNew)
                .start())





def processCmtNew(batch_df, batch_id):
        existDimTime = gold.readData(pathDTime, streaming=False)
        dimTime = gold.createDimTime(existingDimTime=existDimTime, dfNew=batch_df) 
        gold.writeData(dimTime, pathDTime)
        existDimAuthor = gold.readData(pathDAuthor, streaming=False)
        dimAuthor = gold.createDimAuthor(existingAuthor=existDimAuthor, dfNew=batch_df) 
        gold.writeData(dimAuthor, pathDAuthor)

# qCmtNew=(dfCmtNew.writeStream
#                 .option("checkpointLocation","s3a://checkpoint/lakehouse/gold/CmtNew/")
#                 .foreachBatch(processCmtNew)
#                                 .start())



spark.streams.awaitAnyTermination()

