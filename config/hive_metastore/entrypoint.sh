#!/bin/bash

# Fix for Java 11+ IllegalAccessError with Guava and other reflection issues
export HADOOP_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.jar=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED --add-opens java.base/sun.net.www.protocol.http=ALL-UNNAMED --add-opens java.base/sun.net.www.protocol.https=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED ${HADOOP_OPTS}"
export HIVE_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.util.jar=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED --add-opens java.base/sun.net.www.protocol.http=ALL-UNNAMED --add-opens java.base/sun.net.www.protocol.https=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED ${HIVE_OPTS}"

# Dòng xóa SLF4J đã được chuyển vào Dockerfile
# rm -f $HIVE_HOME/lib/slf4j-reload4j-1.7.36.jar || true

echo "Initializing Hive Metastore schema..."
# --verbose giúp bạn thấy chi tiết hơn nếu có lỗi
schematool -dbType mysql -initSchema --verbose || echo "Schema already exists or initialization failed."

echo "Starting Hive Metastore service..."
exec hive --service metastore -p 9083