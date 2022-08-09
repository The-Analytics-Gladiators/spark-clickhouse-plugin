package com.blackmorse.spark.clickhouse

import com.clickhouse.jdbc.ClickHouseDriver

import java.util.Properties

object ClickhouseTests {
  val url = "jdbc:clickhouse://localhost:8123"
  val driver = new ClickHouseDriver()
  lazy val connection = driver.connect(url, new Properties())

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
}
