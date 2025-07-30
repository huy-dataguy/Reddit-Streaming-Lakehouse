test iceberg

1. create database
spark.sql("create database spark_catalog.bronze")
2. spark-submit consumer.py

!! con loi o http request size to minio

3. test da tao duoc iceberg 
=> spark-shell
= = > spark.read.table("spark_catalog.bronze.iceberg_topic").show()