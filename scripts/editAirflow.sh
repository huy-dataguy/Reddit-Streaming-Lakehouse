#!/bin/bash
set -e

FILE="airflow.compose.yaml"

## create folder contain dags logs... for airflow
mkdir -p ../config/airflow/dags ../config/airflow/logs ../config/airflow/plugins ../config/airflow/config
export AIRFLOW_PROJ_DIR="../config/airflow"
AIRFLOW_BASE="../config/airflow"
echo -e "AIRFLOW_UID=$(id -u)" > "$AIRFLOW_BASE/.env"


## insert network in to airflow compose
sed -i '/plugins:/a \ \ networks:\n\ \ \ \ - reddit_network' $FILE

## insert network in postgres
sed -i '/image: postgres:13/a\    networks:\n      - reddit_network' $FILE


## insert network in to redis
sed -i '/image: redis:7.2-bookworm/a\    networks:\n      - reddit_network' $FILE

cat <<EOF >> $FILE

networks:
  reddit_network:
    external: true
EOF
