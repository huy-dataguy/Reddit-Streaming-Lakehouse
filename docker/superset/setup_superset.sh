#!/bin/bash


# create an admin userr
docker exec -it superset superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname admin \
  --email admin@superset.com \
  --password admin 

#init -create default roles, permissions
docker exec -it superset superset init

# Install the Trino driver
docker exec -it superset pip install trino sqlalchemy-trino

docker restart superset

echo "ok! waiting for luv"
