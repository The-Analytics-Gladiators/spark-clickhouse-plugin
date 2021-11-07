#!/bin/bash

set -e
docker network create spark_network
docker run --name spark-clickhouse-server -d --rm --network spark_network -p 8123:8123 yandex/clickhouse-server:21.8.10.19
docker run --name spark-master -d --rm --network spark_network bitnami/spark:3.2.0

#docker network rm spark_network