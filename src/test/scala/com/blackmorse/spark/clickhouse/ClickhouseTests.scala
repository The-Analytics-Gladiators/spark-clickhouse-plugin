package com.blackmorse.spark.clickhouse

import com.clickhouse.jdbc.ClickHouseDriver

import java.util.Properties
import scala.util.Using

object ClickhouseTests {
  def withTable(tableName: String, fields: Seq[String], orderBy: String)(testSpec: => Any) {
    val url = "jdbc:clickhouse://localhost:8123"
    val driver = new ClickHouseDriver()

    Using(driver.connect(url, new Properties())) { connection =>
      connection.createStatement().execute(
        s"""
           |CREATE TABLE $tableName (
           |  ${fields.mkString(", ")}
           |) ENGINE = MergeTree() ORDER BY $orderBy
           |""".stripMargin)
      testSpec
      connection.createStatement().execute(s"DROP TABLE $tableName")
    }
  }
}
