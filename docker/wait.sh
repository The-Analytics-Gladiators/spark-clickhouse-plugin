#! /bin/sh

# Wait for Minio
echo -n 'Waiting for Clickhouse...'
until curl -s http://localhost:8123
do
  sleep 0.5
  echo -n "."
done

until curl -s http://localhost:8124
do
  sleep 0.5
  echo -n "."
done
echo ""
