#!/bin/bash
set -e

SUPERSET_CONTAINER="superset"
SUPERSET_ADMIN_USER="admin"
SUPERSET_ADMIN_PASSWORD="AdminPassword123!"
SUPERSET_ADMIN_EMAIL="admin@example.com"
TRINO_CONN_NAME="Trino"
TRINO_CONN_URI="trino://admin@trino-coordinator:8080/iceberg"

EXPORT_ZIP_PATH="./dashboard/redditdashboard.zip"  

echo "setup Superset"

docker exec -it $SUPERSET_CONTAINER bash -c "
    echo 'init database' &&
    superset db upgrade &&

    echo 'create User' &&
    superset fab create-admin \
      --username $SUPERSET_ADMIN_USER \
      --firstname Superset \
      --lastname Admin \
      --email $SUPERSET_ADMIN_EMAIL \
      --password $SUPERSET_ADMIN_PASSWORD &&

    echo 'init role' &&
    superset init &&
    echo 'install Trino' && 
    pip install trino sqlalchemy-trino &&
    echo 'create connect Trino' &&
    superset set-database-uri \
      --database-name \"$TRINO_CONN_NAME\" \
      --uri \"$TRINO_CONN_URI\"
"

# Import Dashboard Zip
if [ -f "$EXPORT_ZIP_PATH" ]; then
    echo "export dashboard"
    TMP_DIR="./superset_import_tmp"
    rm -rf "$TMP_DIR"
    mkdir -p "$TMP_DIR"
    unzip -q "$EXPORT_ZIP_PATH" -d "$TMP_DIR"

    echo "import dataset"
    if [ -d "$TMP_DIR/datasets" ]; then
        for ds in "$TMP_DIR"/datasets/*.yaml; do
            echo "Import dataset: $ds"
            docker cp "$ds" $SUPERSET_CONTAINER:/tmp/dataset.yaml
            docker exec -it $SUPERSET_CONTAINER superset import-datasets \
                --path /tmp/dataset.yaml \
                --username $SUPERSET_ADMIN_USER
        done
    else
        echo "there no folder datasets in file export."
    fi

    echo "Import dashboards"
    docker cp "$EXPORT_ZIP_PATH" $SUPERSET_CONTAINER:/tmp/redditdashboard.zip
    docker exec -it $SUPERSET_CONTAINER bash -c "
        superset import-dashboards \
            --path /tmp/redditdashboard.zip \
            --username $SUPERSET_ADMIN_USER
    "
else
    echo "not found file export dashboard: $EXPORT_ZIP_PATH"
fi

echo "restart superset"
docker restart $SUPERSET_CONTAINER

echo "success http://localhost:8088"