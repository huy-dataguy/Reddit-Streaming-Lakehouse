#!/bin/bash
set -e

SUPERSET_CONTAINER="superset"
SUPERSET_ADMIN_USER="admin"
SUPERSET_ADMIN_PASSWORD="AdminPassword123!"
SUPERSET_ADMIN_EMAIL="admin@example.com"
TRINO_CONN_NAME="Trino"
TRINO_CONN_URI="trino://admin@trino-coordinator:8080/iceberg"

# EXPORT_ZIP_PATH="../dashboard/my_dashboard.zip"  


echo "=== Thiết lập Superset ==="

docker exec -it $SUPERSET_CONTAINER sh -c "
    echo '=== Khởi tạo database ===' &&
    superset db upgrade &&

    echo '=== Tạo admin user ===' &&
    superset fab create-admin \
      --username $SUPERSET_ADMIN_USER \
      --firstname Superset \
      --lastname Admin \
      --email $SUPERSET_ADMIN_EMAIL \
      --password $SUPERSET_ADMIN_PASSWORD &&

    echo '=== Khởi tạo roles ===' &&
    superset init &&

    echo '=== Cài đặt driver Trino ===' &&
    pip install trino sqlalchemy-trino &&

    echo '=== Tạo kết nối Trino ===' &&
    superset set-database-uri \
      --database-name \"$TRINO_CONN_NAME\" \
      --uri \"$TRINO_CONN_URI\"
"



echo "=== Khởi động lại Superset ==="
docker restart $SUPERSET_CONTAINER

echo "HOÀN TẤT! Truy cập http://localhost:8088"