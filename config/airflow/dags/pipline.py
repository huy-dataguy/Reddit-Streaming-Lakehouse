from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'reddit_streaming_pipeline',
    default_args=default_args,
    description='Streaming Reddit -> Kafka -> Lakehouse -> Superset',
    start_date=datetime(2004, 1, 1),
    catchup=False,
    schedule_interval=None,
)

# start dfs, yarn
startDFS = SSHOperator(
    task_id='start_dfs',
    ssh_conn_id='ssh_spark_master',
    command="bash -lc 'start-dfs.sh'",
    dag=dag,
)
startYARN = SSHOperator(
    task_id='start_yarn',
    ssh_conn_id='ssh_spark_master',
    command="bash -lc 'start-yarn.sh'",
    dag=dag,
)

# create kafka topic reddit Submisison and rediitt comment
createTopic = SSHOperator(
    task_id='create_kafka_topics',
    ssh_conn_id='ssh_kafka',
    command="""
    bash -lc '
    kafka-topics.sh --create --topic redditSubmission --bootstrap-server kafka1:9092 --replication-factor 2 --partitions 6 &&
    kafka-topics.sh --create --topic redditComment --bootstrap-server kafka1:9092 --replication-factor 2 --partitions 6
    '
    """,
    dag=dag,
)

# delete checkpoint in mongodb to track which lines have been processed into kafka
deleteCheckpointMongoDB = SSHOperator(
    task_id='delete_checkpoint',
    ssh_conn_id='ssh_confluent_kafka',
    command="bash -lc 'source /opt/venv/bin/activate && python scripts/delAllDocument.py'",
    dag=dag,
)

# create database, create dim table

createBronzeDB = SSHOperator(
    task_id='create_bronze_db',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'spark-sql -e \"CREATE DATABASE IF NOT EXISTS spark_catalog.bronze\"'",
    dag=dag,
    cmd_timeout=600,
)

createSilverDB = SSHOperator(
    task_id='create_silver_db',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'spark-sql -e \"CREATE DATABASE IF NOT EXISTS spark_catalog.silver\"'",
    dag=dag,
    cmd_timeout=600,
)

createGoldDB = SSHOperator(
    task_id='create_gold_db',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'spark-sql -e \"CREATE DATABASE IF NOT EXISTS spark_catalog.gold\"'",
    dag=dag,
    cmd_timeout=600,
)


createDimTblGold = SSHOperator(
    task_id='create_dim_gold',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'cd ~/spark_submit && spark-submit --py-files transformer.zip,utils.zip createDim.py'",
    dag=dag,
    cmd_timeout=600,
)


# produver mongodb to kafka cluster
produceData = SSHOperator(
    task_id='run_producer',
    ssh_conn_id='ssh_confluent_kafka',
    # command="bash -lc 'source /opt/venv/bin/activate && python scripts/producer.py'",
    command="nohup bash -lc 'source /opt/venv/bin/activate && python scripts/producer.py' > /tmp/producer.log 2>&1 &",

    dag=dag,
    do_xcom_push=False
)

# submit job transform data in lakehouse
# Bronze
sshBronzeSubmit = SSHOperator(
    task_id='submit_bronze_streaming',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'cd ~/spark_submit && spark-submit --py-files utils.zip mainBronze.py'",
    dag=dag,
    do_xcom_push=False,
)

# Silver
submitSilverSub = SSHOperator(
    task_id='submit_silver_submission',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'cd ~/spark_submit && spark-submit --py-files transformer.zip,utils.zip mainRsSilver.py'",
    dag=dag,
    do_xcom_push=False,
)
submitSilverCmt = SSHOperator(
    task_id='submit_silver_comment',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'cd ~/spark_submit && spark-submit --py-files transformer.zip,utils.zip mainRcSilver.py'",
    dag=dag,
    do_xcom_push=False,
)

# Gold
submitGoldDim = SSHOperator(
    task_id='submit_gold_dim',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'cd ~/spark_submit && spark-submit --py-files transformer.zip,utils.zip mainDimGold.py'",
    dag=dag,
    do_xcom_push=False,
)
submitFactPost = SSHOperator(
    task_id='submit_fact_post',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'cd ~/spark_submit && spark-submit --py-files transformer.zip,utils.zip mainFactPost.py'",
    dag=dag,
    cmd_timeout=600,
    do_xcom_push=False,
)


submitFactCmt = SSHOperator(
    task_id='submit_fact_cmt',
    ssh_conn_id='ssh_spark_client',
    command="bash -lc 'cd ~/spark_submit && spark-submit --py-files transformer.zip,utils.zip mainFactCmt.py'",
    dag=dag,
    cmd_timeout=600,
    do_xcom_push=False,
)
# Refresh Superset
# ssh_superset_refresh = SSHOperator(
#     task_id='refresh_superset',
#     ssh_conn_id='ssh_superset',
#     command="bash -lc 'superset refresh-datasources -d spark_catalog'",
#     dag=dag,
#     do_xcom_push=False,
# )


# Start DFS/YARN, Kafka topics, delete checkpoints, producer
deleteCheckpointMongoDB >> createTopic >> startDFS >> startYARN >> [createBronzeDB, createSilverDB, createGoldDB] >> createDimTblGold

# Producer,submit streaming jobs (Bronze,Silver,Gold in parallel)
createDimTblGold >> produceData >> sshBronzeSubmit
sshBronzeSubmit >> [submitSilverSub, submitSilverCmt] >> submitGoldDim >> [submitFactPost,submitFactCmt]

# Refresh Superset
# [submitSilverSub, submitSilverCmt, submitGoldDim, submitGoldFact] >> ssh_superset_refresh
