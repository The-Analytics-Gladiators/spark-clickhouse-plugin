#! /bin/sh

# Wait for Minio
until curl -s http://localhost:8123
do
  echo 'Waiting for Clickhouse...'
  sleep 4
done
curl -vvv http://localhost:8123

