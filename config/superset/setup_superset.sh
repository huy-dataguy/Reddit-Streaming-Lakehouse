#!/bin/bash
set -e

SUPERSET_CONTAINER="superset"
SUPERSET_ADMIN_USER="admin"
SUPERSET_ADMIN_PASSWORD="AdminPassword123!"
SUPERSET_ADMIN_EMAIL="admin@example.com"
TRINO_CONN_NAME="Trino"
TRINO_CONN_URI="trino://admin@trino-coordinator:8080/iceberg"

EXPORT_ZIP_PATH="./dashboard/redditdashboard.zip"  

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

# =========================
# Import Dashboard Zip
# =========================
if [ -f "$EXPORT_ZIP_PATH" ]; then
    echo "=== Giải nén file export dashboard ==="
    TMP_DIR="./superset_import_tmp"
    rm -rf "$TMP_DIR"
    mkdir -p "$TMP_DIR"
    unzip -q "$EXPORT_ZIP_PATH" -d "$TMP_DIR"

    echo "=== Import datasets trước ==="
    if [ -d "$TMP_DIR/datasets" ]; then
        for ds in "$TMP_DIR"/datasets/*.yaml; do
            echo "Import dataset: $ds"
            docker cp "$ds" $SUPERSET_CONTAINER:/tmp/dataset.yaml
            docker exec -it $SUPERSET_CONTAINER superset import-datasets \
                --path /tmp/dataset.yaml \
                --username $SUPERSET_ADMIN_USER
        done
    else
        echo "Không tìm thấy thư mục datasets trong file export."
    fi

    echo "=== Import dashboards sau ==="
    docker cp "$EXPORT_ZIP_PATH" $SUPERSET_CONTAINER:/tmp/redditdashboard.zip
    docker exec -it $SUPERSET_CONTAINER sh -c "
        superset import-dashboards \
            --path /tmp/redditdashboard.zip \
            --username $SUPERSET_ADMIN_USER
    "
else
    echo "Không tìm thấy file export dashboard: $EXPORT_ZIP_PATH"
fi

echo "=== Khởi động lại Superset ==="
docker restart $SUPERSET_CONTAINER

echo "HOÀN TẤT! Truy cập http://localhost:8088"