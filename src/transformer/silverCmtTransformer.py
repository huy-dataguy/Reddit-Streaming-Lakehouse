from pyspark.sql import functions as F
from transformer.silverBaseTransformer import BaseTransformer



class CommentTransformer(BaseTransformer):
    def __init__(self, sparkSession):
        super().__init__(sparkSession)

    def normalizeParentId(self, df, parentIdCol="parent_id", newCol="parent_clean"):
        """Removes prefixes like 't3_' and 't1_' from parent_id."""
        return df.withColumn(
            newCol,
            F.regexp_replace(F.col(parentIdCol), r"^(t[13]_)", "")
        )

    def normalizeLinkId(self, df, linkIdCol="link_id", newCol="link_clean"):
        """Removes the 't3_' prefix from link_id."""
        return df.withColumn(
            newCol,
            F.regexp_replace(F.col(linkIdCol), r"^t3_", "")
        )
    
    def markModComments(self, df, authorCol="author", newColMod="deleted_by_mod", newColAutoMod="deleted_by_auto"):
        """Marks comments from Mod Team or AutoModerator."""
        df_with_mod = df.withColumn(
            newColMod,
            F.lower(F.col(authorCol)).like("%-modteam")
        )
        return df_with_mod.withColumn(
            newColAutoMod,
            F.col(authorCol) == "AutoModerator"
        )
    def transform(self,df):
        df = self.convertTimestamp(df)
        df = self.markAuthorDeleted(df) 
        df = self.markBodyRemoved(df, body="body", newCol="cmtDeleted")
        df = self.normalizeParentId(df)
        df = self.normalizeLinkId(df)
        df = self.markModComments(df)
        return df