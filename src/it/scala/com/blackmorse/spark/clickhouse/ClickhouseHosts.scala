package com.blackmorse.spark.clickhouse

object ClickhouseHosts {
  case class ClickhouseHost(hostName: String, port: Int) {
    override def toString: String = s"$hostName:$port"
  }

  val shard1Replica1: ClickhouseHost = ClickhouseHost("clickhouse-server", 8123)
  val shard1Replica2: ClickhouseHost = ClickhouseHost("clickhouse-server-replica", 8124)
  val shard2Replica1: ClickhouseHost = ClickhouseHost("clickhouse-server-second-shard", 8125)

  val clusterName: String = "spark_clickhouse_cluster"
  val testTable: String = "default.test_table"
  val clusterTestTable: String = "default.cluster_test_table"
}
