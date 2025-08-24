from transformer.silverBaseTransformer import BaseTransformer
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
from transformer.sentimentModel import sentimentUdf



class GoldTransformer(BaseTransformer):
    def __init__(self, sparkSession, dfSubmission, dfComment):
        super().__init__(sparkSession)
        self.dfSubmission = dfSubmission
        self.dfComment = dfComment
    
    def createDimTime(self,timestampCol="createdDate"):
        dfTimestamp=(self.dfSubmission.select(timestampCol).distinct().union(self.dfComment.select(timestampCol).distinct())).distinct()
        dimTime=(dfTimestamp.withColumn("year", F.year(F.col(timestampCol)))
                            .withColumn("month", F.month(F.col(timestampCol)))
                            .withColumn("day", F.dayofmonth(F.col(timestampCol)))
                            .withColumn("hour", F.hour(F.col(timestampCol)))
                            .withColumn("minute", F.minute(F.col(timestampCol))) 
                            .withColumn("second", F.second(F.col(timestampCol))) 
                            .withColumn("day_of_week", F.dayofweek(F.col(timestampCol)))
                            .withColumnRenamed("createdDate", "time_key"))
        return dimTime
        
    def createDimAuthor(self, authorName="author", fullName="author_fullname"):
        dimAuthor=(self.dfSubmission.select(authorName, fullName).distinct()
            .union(self.dfComment.select(authorName, fullName).distinct())
            .distinct()
            .withColumnRenamed("author_fullname", "author_key"))
        return dimAuthor

    def createDimSubreddit(self, subredditId="subreddit_id", subredditName="subreddit", subredditNamePrefixed="subreddit_name_prefixed", subredditType="subreddit_type"):
        dimSubreddit=(self.dfSubmission.select(
                                        F.col(subredditId),
                                        F.col(subredditName),
                                        F.col(subredditNamePrefixed),
                                        F.col(subredditType))
                                    .withColumnRenamed("subreddit_id", "subreddit_key")
                                      .distinct())
        return dimSubreddit

    def createDimPostType(self, postType="post_hint"):
        dimPostType=(self.dfSubmission.select(F.col(postType)).distinct()
            .withColumn("postType_key", F.monotonically_increasing_id()))
        return dimPostType
    
    def createDimSentiment(self):
        sentiment_labels = ["negative", "positive", "neutral"]
        data = [(label,) for label in sentiment_labels]
        sentiment_schema = StructType([StructField("sentiment_label", StringType(), True)])
        
        dimSentiment = (self.spark.createDataFrame(data, schema=sentiment_schema)
                        .withColumn("sentiment_key", F.monotonically_increasing_id()))
        return dimSentiment

    def createDimPost (self):
        dfSub = self.dfSubmission

        
        dfCmt=self.dfComment

        dfCmt = dfCmt.select("*").where((F.col("deleted_by_mod") == True) | (F.col("deleted_by_auto") == True)).dropDuplicates(["link_clean"])

        dimJoinRaw = (
            dfSub.join(dfCmt, dfSub["id"] == dfCmt["link_clean"], "left")
                  .select(
                      dfSub["id"],
                      dfSub["title"],
                      dfSub["selftext"],
                      dfSub["permalink"],
                      dfSub["url"],
                      dfSub["domain"],
                      dfSub["edited"],
                      dfSub["locked"],
                      dfSub["spoiler"],
                      dfSub["over_18"],
                      dfSub["stickied"],
                      dfSub["is_original_content"],
                      dfSub["link_flair_text"],
                      dfSub["accDeleted"],
                      dfSub["isPostSpam"],
                      dfSub["matchPercent"],
                      dfSub["postDeleted"],
                      dfCmt["deleted_by_mod"],
                      dfCmt["deleted_by_auto"]
                  )
        )                                                             
        dimPost=(dimJoinRaw.withColumn("postStatus",
                                         F.when(F.col("accDeleted")==True, F.lit("AuthorDeleted"))
                                          .when((F.col ("postDeleted")==True) & (F.col("accDeleted")==False), F.lit("SelfDeleted"))
                                         .when(F.col("deleted_by_mod")==True, F.lit("ModDeleted"))
                                         .when(F.col("deleted_by_auto")==True, F.lit("AutoDeleted"))
                                            .otherwise(F.lit("active")))
                                .withColumnRenamed("selftext", "body")
                                .drop("deleted_by_mod", "deleted_by_auto")
                                        .withColumnRenamed("id", "post_key"))
        


        return dimPost
    
    def createDimComment(self):
        dfCmt=self.dfComment
       
        
        dimComment=(dfCmt.select(F.col("id"), F.col("body"), F.col("permalink")
                                         , F.col("edited"), F.col("is_submitter")
                                         , F.col("controversiality"))
                                        # , F.col("sentiment_label"))
                                .withColumnRenamed("id", "comment_key"))

        
        return dimComment



    def createFactPostActivity(self,dimTime, dimAuthor, dimSubreddit, dimPostType, dimSentiment):
        
        dfSub = (self.dfSubmission
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
            .withColumn("id", F.monotonically_increasing_id()))
        return factActi
        
    def createFactCommentActivity(self, dimTime, dimAuthor, dimSubreddit, dimPost, dimSentiment):
         
        dfCmt = self.dfComment.withColumn(
            "sentiment_label", sentimentUdf(F.col("body"))
        )

        
        dfCommentActi = (dfCmt.select(
            F.col("id"),col("author_fullname"),col("createdDate"),col("subreddit"),
            col("link_clean"),
            col("sentiment_label"),
            col("score"),
            col("controversiality"),
            col("total_awards_received")))

        factActi = dfCommentActi.join(dimTime,
            dimTime.time_key == dfCommentActi.createdDate,
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
            .withColumn("id", F.monotonically_increasing_id()))

        return factActi
