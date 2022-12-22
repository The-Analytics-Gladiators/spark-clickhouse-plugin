package com.blackmorse.spark.clickhouse.tables.services

import com.blackmorse.spark.clickhouse.utils.JDBCUtils

import java.util.Properties
import scala.util.Try

object ClusterService {
  def getShardUrls(url: String, cluster: String, connectionsProps: Properties): Try[Seq[String]] = {
    //Figuring out http ports of remote clickhouse hosts is not so trivial:
    val sql =
      s"""
         |SELECT
         |    host_name || ':' || getServerPort('http_port')::String AS http_port
         |FROM clusterAllReplicas($cluster, system, clusters)
         |WHERE is_local = 1
         |LIMIT 1 BY shard_num
         |""".stripMargin

    JDBCUtils.executeSql(url, connectionsProps)(sql) { rs => rs.getString(1) }
  }
}
