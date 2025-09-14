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
pathDPost = "spark_catalog.gold.dimPost"
pathDComment = "spark_catalog.gold.dimComment"

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

        # existDimPostType =gold.readData(pathDPostType)
        # dimPostType = gold.createDimPostType(existingPostType=existDimPostType, dfNew=batch_df)
        # gold.writeData(dimPostType, pathDPostType)

        existDimPost =gold.readData(pathDPost)
        dimPost = gold.createDimPost(dfSubNew=batch_df, existDPost=existDimPost)
        gold.writeData(dimPost, pathDPost)

# qSubNew=(dfSubNew.writeStream
#                 .option("checkpointLocation","s3a://checkpoint/lakehouse/gold/subNewww/")
#                 .foreachBatch(processSubNew)
#                 .start())





def processCmtNew(batch_df, batch_id):
        # existDimTime = gold.readData(pathDTime, streaming=False)
        # dimTime = gold.createDimTime(existingDimTime=existDimTime, dfNew=batch_df) 
        # gold.writeData(dimTime, pathDTime)
        # existDimAuthor = gold.readData(pathDAuthor, streaming=False)
        # dimAuthor = gold.createDimAuthor(existingAuthor=existDimAuthor, dfNew=batch_df) 
        # gold.writeData(dimAuthor, pathDAuthor)

        # existDimPost =gold.readData(pathDPost)
        # dimPostUpdate =gold.updateDimPostStatus(dimPostExist=existDimPost, dfCmtNew=batch_df)
        # gold.writeData(dimPostUpdate, pathDPost, mode="overwrite")

        existDimComment=gold.readData(pathDComment)
        dimComment = gold.createDimComment(existingCmt=existDimComment, dfCmtNew=batch_df)
        gold.writeData(dimComment,pathDComment)
qCmtNew=(dfCmtNew.writeStream
                .option("checkpointLocation","s3a://checkpoint/lakehouse/gold/CmtNeweeewwrwwwww/")
                .foreachBatch(processCmtNew)
                                .start())



spark.streams.awaitAnyTermination()

