where to put dockerfile of functional components\

ex:
- dockerfile.spark.yaml
- dockerfile.trino.yaml


#### 1.
docker build -t sparkbase -f base.dockerfile .


$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  $SPARK_HOME/examples/jars/spark-examples_2.12-3.5.6.jar \
  10


