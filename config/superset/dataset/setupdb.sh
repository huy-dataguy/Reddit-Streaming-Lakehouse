#!/bin/bash
set -e
#config
TRINO_CONTAINER="trino-coordinator"   
TRINO_HOST="trino-coordinator"        
TRINO_PORT="8080"
CATALOG="iceberg"
SCHEMA="gold"
TRINO_USER="admin"

YAML_OUTPUT="reddit_dataset.yaml"

# map data type from trino to superset
map_data_type() {
    local trino_type="$1"
    trino_type=$(echo "$trino_type" | tr '[:upper:]' '[:lower:]')

    case "$trino_type" in
        varchar*|char*|string) echo "STRING" ;;
        bigint|integer|int)    echo "BIGINT" ;;
        double|float|real|decimal*) echo "DOUBLE" ;;
        boolean)               echo "BOOLEAN" ;;
        timestamp*|date)       echo "TIMESTAMP" ;;
        *)                     echo "STRING" ;; 
    esac
}

# get tables
echo "get table from Trino"
TABLES=$(docker exec -i ${TRINO_CONTAINER} trino \
  --server ${TRINO_HOST}:${TRINO_PORT} \
  --catalog ${CATALOG} \
  --schema ${SCHEMA} \
  --user ${TRINO_USER} \
  --output-format=TSV \
  --execute "SHOW TABLES" \
  | grep -v -E "Table|----|^$")

echo "found table:"
echo "$TABLES"

# init yaml
rm -f "$YAML_OUTPUT"
{
  echo "databases:"
  echo "  - database_name: Trino"
  echo "    tables:"
} >> "$YAML_OUTPUT"

# create view and record view
for TABLE in $TABLES; do
    VIEW_NAME="${TABLE}_view"
    echo "create view: $VIEW_NAME"

    docker exec -i ${TRINO_CONTAINER} trino \
      --server ${TRINO_HOST}:${TRINO_PORT} \
      --catalog ${CATALOG} \
      --schema ${SCHEMA} \
      --user ${TRINO_USER} \
      --execute "CREATE OR REPLACE VIEW ${VIEW_NAME} AS SELECT * FROM ${TABLE}"

    #get columns and data type
    COLUMNS_INFO=$(docker exec -i ${TRINO_CONTAINER} trino \
      --output-format=TSV \
      --server ${TRINO_HOST}:${TRINO_PORT} \
      --catalog ${CATALOG} \
      --schema ${SCHEMA} \
      --user ${TRINO_USER} \
      --execute "SHOW COLUMNS FROM ${VIEW_NAME}" \
      | tail -n +2)

    {
      echo "      - table_name: ${VIEW_NAME}"
      echo "        schema: ${SCHEMA}"
      echo "        columns:"
    } >> "$YAML_OUTPUT"

    while IFS=$'\t' read -r col_name col_type _; do
        superset_type=$(map_data_type "$col_type")
        echo "          - column_name: ${col_name}" >> "$YAML_OUTPUT"
        echo "            type: ${superset_type}" >> "$YAML_OUTPUT"
        echo "            groupby: true" >> "$YAML_OUTPUT"
        echo "            filterable: true" >> "$YAML_OUTPUT"
        if [[ "$superset_type" == "TIMESTAMP" ]]; then
            echo "            is_dttm: true" >> "$YAML_OUTPUT"
        fi
    done <<< "$COLUMNS_INFO"
done

echo " success File YAML: $YAML_OUTPUT"