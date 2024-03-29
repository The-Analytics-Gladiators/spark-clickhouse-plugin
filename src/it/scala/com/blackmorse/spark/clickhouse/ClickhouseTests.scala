package com.blackmorse.spark.clickhouse

import com.clickhouse.jdbc.ClickHouseDriver
import com.blackmorse.spark.clickhouse.ClickhouseHosts._

import java.util.Properties

object ClickhouseTests {
  val url = s"jdbc:clickhouse://$shard1Replica1"
  val driver = new ClickHouseDriver()
  private lazy val connection = driver.connect(url, new Properties())


  def withTable(fields: Seq[String],
                 orderBy: String,
                 tableEngine: String = "MergeTree()")(testSpec: => Any) {
    val statement = connection.createStatement()
    val sign = if (isCollapsingMergeTreeEngine(tableEngine)) s"(${fields.last.split(" ")(0)})" else ""
    try {
      statement.execute(
        s"""
           |CREATE TABLE $testTable (
           |  ${fields.mkString(", ")}
           |) ENGINE = $tableEngine$sign ORDER BY $orderBy
           |""".stripMargin)
      testSpec
    } finally {
      statement.close()
      connection.createStatement().execute(s"DROP TABLE IF EXISTS $testTable SYNC")
    }
  }

  def withClusterTable(fields: Seq[String],
                       orderBy: String,
                       withDistributed: Boolean,
                       tableEngine: String = "ReplicatedMergeTree")(testSpec: => Any): Unit = {
    val statement = connection.createStatement()
    val sign = if (isCollapsingMergeTreeEngine(tableEngine)) s", ${fields.last.split(" ")(0)}" else ""
    try {
      statement.execute(
        s"""
           |CREATE TABLE $clusterTestTable on cluster $clusterName (
           |  ${fields.mkString(", ")}
           |) ENGINE = $tableEngine('/clickhouse/tables/{shard}/$clusterTestTable', '{replica}'$sign)
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

  private def isCollapsingMergeTreeEngine(tableEngine: String): Boolean = {
    tableEngine.contains("CollapsingMergeTree")
  }
}
