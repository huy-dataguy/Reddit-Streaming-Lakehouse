
from utils.getSparkConfig import getSparkConfig


spark = getSparkConfig("CreateDimTable")

spark.sql("""
--DimSentiment
CREATE TABLE IF NOT EXISTS spark_catalog.gold.dimsentiment (
  sentiment_label STRING,
  sentiment_key BIGINT
)
USING iceberg
LOCATION 's3a://lakehouse/gold.db/dimSentiment'
TBLPROPERTIES (
  'format-version' = '2',
  'format' = 'iceberg/parquet',
  'write.parquet.compression-codec' = 'zstd'
);
""")

spark.sql("""
--DimTime
CREATE TABLE IF NOT EXISTS spark_catalog.gold.dimtime (
  time_key TIMESTAMP,
  year INT,
  month INT,
  day INT,
  hour INT,
  minute INT,
  second INT,
  day_of_week INT
)
USING iceberg
LOCATION 's3a://lakehouse/gold.db/dimTime'
TBLPROPERTIES (
  'format-version' = '2',
  'format' = 'iceberg/parquet',
  'write.parquet.compression-codec' = 'zstd'
);

""")
spark.sql("""

--DimAuthor
CREATE TABLE IF NOT EXISTS spark_catalog.gold.dimauthor (
  author STRING,
  author_key STRING
)
USING iceberg
LOCATION 's3a://lakehouse/gold.db/dimAuthor'
TBLPROPERTIES (
  'format-version' = '2',
  'format' = 'iceberg/parquet',
  'write.parquet.compression-codec' = 'zstd'
);


""")
spark.sql("""
--DimSubreddit
CREATE TABLE IF NOT EXISTS spark_catalog.gold.dimsubreddit (
  subreddit_key STRING,
  subreddit STRING,
  subreddit_name_prefixed STRING,
  subreddit_type STRING
)
USING iceberg
LOCATION 's3a://lakehouse/gold.db/dimSubreddit'
TBLPROPERTIES (
  'format-version' = '2',
  'format' = 'iceberg/parquet',
  'write.parquet.compression-codec' = 'zstd'
);


""")
spark.sql("""

--DimPostType
CREATE TABLE IF NOT EXISTS spark_catalog.gold.dimposttype (
  post_hint STRING,
  postType_key STRING
)
USING iceberg
LOCATION 's3a://lakehouse/gold.db/dimPostType'
TBLPROPERTIES (
  'format-version' = '2',
  'format' = 'iceberg/parquet',
  'write.parquet.compression-codec' = 'zstd'
);

""")
spark.sql("""

--Dimpost
CREATE TABLE IF NOT EXISTS spark_catalog.gold.dimpost (
  post_key STRING,
  title STRING,
  body STRING,
  permalink STRING,
  url STRING,
  domain STRING,
  edited BOOLEAN,
  locked BOOLEAN,
  spoiler BOOLEAN,
  over_18 BOOLEAN,
  stickied BOOLEAN,
  is_original_content BOOLEAN,
  link_flair_text STRING,
  accDeleted BOOLEAN,
  isPostSpam BOOLEAN,
  matchPercent DOUBLE,
  postDeleted BOOLEAN
--   postStatus STRING

)
USING iceberg
LOCATION 's3a://lakehouse/gold.db/dimPost'
TBLPROPERTIES (
  'format-version' = '2',
  'format' = 'iceberg/parquet',
  'write.parquet.compression-codec' = 'zstd'
);

""")
spark.sql("""


--DimComment
CREATE TABLE IF NOT EXISTS spark_catalog.gold.dimcomment (
  comment_key STRING,
  body STRING,
  permalink STRING,
  edited BOOLEAN,
  is_submitter BOOLEAN,
  controversiality INT,
  parent_clean STRING,
  link_clean STRING,
  deleted_by_mod BOOLEAN,
  deleted_by_auto BOOLEAN
)
USING iceberg
LOCATION 's3a://lakehouse/gold.db/dimComment'
TBLPROPERTIES (
  'format-version' = '2',
  'format' = 'iceberg/parquet',
  'write.parquet.compression-codec' = 'zstd'
);

""")
