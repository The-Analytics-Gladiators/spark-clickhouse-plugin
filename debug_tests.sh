#!/bin/bash
# In IDEA: Debug Remote JVM, port 5005, attach to remote JVM. Params: -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
docker build -t sbt_ch_plugin:latest . && docker rm sbt-clickhouse-plugin && docker run -it -p 5005:5005  --name sbt-clickhouse-plugin --network ch_network sbt_ch_plugin:latest sbt test it:test
