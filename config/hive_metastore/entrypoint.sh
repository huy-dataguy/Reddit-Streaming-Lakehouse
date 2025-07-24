#!/bin/bash

echo "Initializing Hive Metastore schema..."
schematool -dbType mysql -initSchema --verbose || echo "Schema already exists or initialization failed."

echo "Starting Hive Metastore service..."
exec hive --service metastore -p 9083

exec bash -c "tail -F anything"
