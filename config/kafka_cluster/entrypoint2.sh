#!/bin/bash

set -e
service ssh start

exec su confluent_kafka_user bash -c "tail -F anything"