#! /bin/sh

function wait_for_clickhouse() {
  url=$1
  echo -n "Waiting for Clickhouse $url ..."
  until curl -s $url
  do
    sleep 0.5
    echo -n "."
  done
}

wait_for_clickhouse http://localhost:8123
wait_for_clickhouse http://localhost:8124
wait_for_clickhouse http://localhost:8125