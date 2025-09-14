from transformer.silverBaseTransformer import BaseTransformer
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from transformer.sentimentModel import sentimentUdf
from pyspark.sql.functions import expr



class GoldTransformer(BaseTransformer):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)

    
    def createDimTime(self,timestampCol="createdDate", existingDimTime=None, dfNew=None):

        dfTimestamp=dfNew.select(timestampCol).distinct()

        if existingDimTime is not None:
            dfTimestamp = dfTimestamp.join(
                existingDimTime.select("time_key"),
                dfTimestamp[timestampCol] == existingDimTime["time_key"],
                "left_anti"  
            )


        dimTime=(dfTimestamp.withColumn("year", F.year(F.col(timestampCol)))
                            .withColumn("month", F.month(F.col(timestampCol)))
                            .withColumn("day", F.dayofmonth(F.col(timestampCol)))
                            .withColumn("hour", F.hour(F.col(timestampCol)))
                            .withColumn("minute", F.minute(F.col(timestampCol))) 
                            .withColumn("second", F.second(F.col(timestampCol))) 
                            .withColumn("day_of_week", F.dayofweek(F.col(timestampCol)))
                            .withColumnRenamed("createdDate", "time_key"))
        return dimTime
        
    def createDimAuthor(self, authorName="author", fullName="author_fullname", existingAuthor=None, dfNew=None):
        dimAuthor = (
            dfNew.filter(F.col("accDeleted") == False)
                .select(authorName, fullName)
                .distinct()
                .withColumnRenamed("author_fullname", "author_key"))

        
        if existingAuthor is not None:
            dimAuthor = dimAuthor.join(
                existingAuthor.select("author_key"),
                dimAuthor["author_key"] == existingAuthor["author_key"],
                "left_anti"
            )
        return dimAuthor

    def createDimSubreddit(self, subredditId="subreddit_id", subredditName="subreddit", subredditNamePrefixed="subreddit_name_prefixed", subredditType="subreddit_type", existingSubreddit=None, dfNew=None):
        dimSubreddit = (
            dfNew.select(
                F.col(subredditId),
                F.col(subredditName),
                F.col(subredditNamePrefixed),
                F.col(subredditType)
            )
            .withColumnRenamed(subredditId, "subreddit_key")
            .distinct()
        )
        if existingSubreddit is not None:
            dimSubreddit = dimSubreddit.join(
                existingSubreddit.select("subreddit_key"),
                dimSubreddit["subreddit_key"] == existingSubreddit["subreddit_key"],
                "left_anti"
            )
        return dimSubreddit

    def createDimPostType(self, postType="post_hint", existingPostType=None, dfNew=None):

        dfNew = dfNew.withColumn(
        postType,
        F.when(F.col(postType).isNull(), F.lit("UNKNOWN")).otherwise(F.col(postType)))

        dimPostType = (
            dfNew.select(F.col(postType)).distinct()
            .withColumn("postType_key", expr("uuid()")))

        if existingPostType is not None:
            dimPostType = dimPostType.join(
            existingPostType.select("post_hint"),
            dimPostType[postType] == existingPostType["post_hint"],
            "left_anti"
            )

        return dimPostType
    
    
    def createDimSentiment(self):
        sentiment_labels = ["negative", "positive", "neutral"]
        data = [(label,) for label in sentiment_labels]
        sentiment_schema = StructType([StructField("sentiment_label", StringType(), True)])
        
        dimSentiment = (self.spark.createDataFrame(data, schema=sentiment_schema)
                        .withColumn("sentiment_key", F.monotonically_increasing_id()))
        return dimSentiment

    def createDimPost(self, dfSubNew, existDPost=None):
        newPosts = (
            dfSubNew.select(
                "id",
                "title",
                "selftext",
                "permalink",
                "url",
                "domain",
                "edited",
                "locked",
                "spoiler",
                "over_18",
                "stickied",
                "is_original_content",
                "link_flair_text",
                "accDeleted",
                "isPostSpam",
                "matchPercent",
                "postDeleted"
            )
            .withColumnRenamed("selftext", "body")
            .withColumnRenamed("id", "post_key")
            .distinct()
        )

        if existDPost is not None:
            newPosts = newPosts.join(
                existDPost.select("post_key"),
                newPosts["post_key"] == existDPost["post_key"],
                "left_anti"
            )

        return newPosts
    

    

    
    def updateDimPostStatus(self, dimPostExist=None, dfCmtNew=None):
        dfCmtFlag = (
            dfCmtNew
            .filter((F.col("deleted_by_mod") == True) | (F.col("deleted_by_auto") == True))
            .dropDuplicates(["link_clean"])
        )

        dimJoinRaw = (
            dimPostExist.alias("p")
            .join(dfCmtFlag.alias("c"), F.col("p.post_key") == F.col("c.link_clean"), "left")
            .select(
                "p.post_key",
                "p.title",
                "p.body",
                "p.permalink",
                "p.url",
                "p.domain",
                "p.edited",
                "p.locked",
                "p.spoiler",
                "p.over_18",
                "p.stickied",
                "p.is_original_content",
                "p.link_flair_text",
                "p.accDeleted",
                "p.isPostSpam",
                "p.matchPercent",
                "p.postDeleted",
                "c.deleted_by_mod",
                "c.deleted_by_auto"
            )
            .distinct()
        )

        return (
            dimJoinRaw.withColumn(
                "postStatus",
                F.when(F.col("accDeleted") == True, F.lit("AuthorDeleted"))
                .when((F.col("postDeleted") == True) & (F.col("accDeleted") == False), F.lit("SelfDeleted"))
                .when(F.col("deleted_by_mod") == True, F.lit("ModDeleted"))
                .when(F.col("deleted_by_auto") == True, F.lit("AutoDeleted"))
                .otherwise(F.lit("Active"))
            )
            .drop("deleted_by_mod", "deleted_by_auto")
        )

    
    def createDimComment(self, existingCmt=None, dfCmtNew=None):
        dimComment=(dfCmtNew.select(F.col("id"), F.col("body"), F.col("permalink")
                                         , F.col("edited"), F.col("is_submitter")
                                         , F.col("controversiality"))
                                        # , F.col("sentiment_label"))
                                .withColumnRenamed("id", "comment_key"))
        if existingCmt is not None:
            dimComment = dimComment.join(
                existingCmt.select("comment_key"),
                dimComment["comment_key"] == existingCmt["comment_key"],
                "left_anti"
            )

        return dimComment



    def createFactPostActivity(self,dimTime, dimAuthor, dimSubreddit, dimPostType, dimSentiment, dfSubNew, dfCmtNew):
        
        dfSub = (dfSubNew
            .withColumn("text", F.concat_ws(" ", F.col("title"), F.col("selftext")))
            .withColumn("sentiment_label", sentimentUdf(F.col("text")))
        )

        dfPostActi = (dfSub.select(F.col("id"),  col("author_fullname"), col("createdDate"),
                                                col("subreddit"), col("post_hint"),
                                                col("score"), col("num_comments"),
                                               col("total_awards_received"),
                                               col("sentiment_label"),
                                              col("subreddit_subscribers")))
        
        factActi=dfPostActi.join(dimTime, dimTime.time_key==dfPostActi.createdDate,
                                 "left").drop("createdDate")
        factActi=factActi.join(dimAuthor, dimAuthor.author_key==factActi.author_fullname,
                               "left").drop("author_fullname")
        factActi=factActi.join(dimSubreddit, dimSubreddit.subreddit==factActi.subreddit, "left").drop("subreddit")
        factActi=factActi.join(dimPostType, dimPostType.post_hint==factActi.post_hint, "left").drop("post_hint")

        factActi=factActi.join(dimSentiment, dimSentiment.sentiment_label==factActi.sentiment_label, 
                              "left").drop("sentiment_label")

        factActi = (factActi.select(
            F.col("id"), col("time_key"), col("sentiment_key"), col("author_key"),col("subreddit_key"),col("postType_key"),
           col("score"),col("num_comments"),col("total_awards_received"),col("subreddit_subscribers"))
            .withColumnRenamed("id", "post_key")
            .withColumn("id", expr("uuid()")))
        return factActi
        
    def createFactCommentActivity(self, dimTime, dimAuthor, dimSubreddit, dimPost, dimSentiment, dfSubNew, dfCmtNew):
         
        dfCmt = dfCmtNew.withColumn(
            "sentiment_label", sentimentUdf(F.col("body"))
        )

        
        dfCmtNewActi = (dfCmt.select(
            F.col("id"),col("author_fullname"),col("createdDate"),col("subreddit"),
            col("link_clean"),
            col("sentiment_label"),
            col("score"),
            col("controversiality"),
            col("total_awards_received")))

        factActi = dfCmtNewActi.join(dimTime,
            dimTime.time_key == dfCmtNewActi.createdDate,
            "left").drop("createdDate")
        factActi= factActi.join(dimAuthor,
            dimAuthor.author_key == factActi.author_fullname,
            "left").drop("author_fullname")
    
        factActi =factActi.join(dimSubreddit,
            dimSubreddit.subreddit == factActi.subreddit,
            "left").drop("subreddit")

        factActi =factActi.join(dimPost,
            dimPost.post_key == factActi.link_clean,
            "left").drop("link_clean")
        
        factActi = factActi.join(
            dimSentiment,
            dimSentiment.sentiment_label == factActi.sentiment_label,
            "left").drop("sentiment_label")
        

        factActi= (factActi.select(
            F.col("id"),
            F.col("time_key"),col("author_key"), col("sentiment_key"), col("subreddit_key"),
            col("post_key"),
            col("score"),col("controversiality"), col("total_awards_received"))
            .withColumnRenamed("id", "comment_key")
            .withColumn("id", expr("uuid()")))

        return factActi
