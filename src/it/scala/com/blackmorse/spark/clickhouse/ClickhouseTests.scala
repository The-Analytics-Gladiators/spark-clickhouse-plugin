package com.blackmorse.spark.clickhouse

import com.clickhouse.jdbc.ClickHouseDriver
import com.blackmorse.spark.clickhouse.ClickhouseHosts._

import java.util.Properties

object ClickhouseTests {
  val url = s"jdbc:clickhouse://$shard1Replica1"
  val driver = new ClickHouseDriver()
  private lazy val connection = driver.connect(url, new Properties())

  def withTable(fields: Seq[String], orderBy: String)(testSpec: => Any) {
    val tableName = "default.test_table"
    val statement = connection.createStatement()
    try {
      statement.execute(
        s"""
           |CREATE TABLE $tableName (
           |  ${fields.mkString(", ")}
           |) ENGINE = MergeTree() ORDER BY $orderBy
           |""".stripMargin)
      testSpec
    } finally {
      statement.close()
      connection.createStatement().execute(s"DROP TABLE IF EXISTS $tableName SYNC")
    }
  }

  def withClusterTable(fields: Seq[String], orderBy: String, withDistributed: Boolean)(testSpec: => Any): Unit = {
    val statement = connection.createStatement()
    try {
      statement.execute(
        s"""
           |CREATE TABLE $clusterTestTable on cluster $clusterName (
           |  ${fields.mkString(", ")}
           |) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$clusterTestTable', '{replica}')
           |ORDER BY $orderBy
           |""".stripMargin
      )
      if (withDistributed) {

        statement.execute(
          s"""
             |CREATE TABLE $clusterDistributedTestTable on cluster $clusterName (
             |  ${fields.mkString(", ")}
             |) ENGINE = Distributed('$clusterName', '${clusterTestTable.split("\\.").head}', '${clusterTestTable.split("\\.")(1)}')
             |""".stripMargin
        )
      }
      testSpec
    } finally {
      statement.close()
      connection.createStatement().execute(s"DROP TABLE IF EXISTS $clusterTestTable ON CLUSTER $clusterName SYNC")
      if (withDistributed) {
        connection.createStatement().execute(s"DROP TABLE IF EXISTS $clusterDistributedTestTable ON CLUSTER $clusterName SYNC")
      }
    }
  }
}
