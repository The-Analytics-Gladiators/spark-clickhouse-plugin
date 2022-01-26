package com.blackmorse.spark.clickhouse

import com.clickhouse.jdbc.ClickHouseDriver
import ru.yandex.clickhouse.settings.ClickHouseProperties

import java.util.Properties
import scala.util.{Failure, Success, Using}

object ClickhouseTests {
  def withTable(fields: Seq[String], orderBy: String)(testSpec: => Any) {
    val url = "jdbc:clickhouse://localhost:8123"
    val driver = new ClickHouseDriver()
    val tableName = "default.test_table"

    Using(driver.connect(url, new Properties())) { connection =>
      try {
        connection.createStatement().execute(
          s"""
             |CREATE TABLE $tableName (
             |  ${fields.mkString(", ")}

             |) ENGINE = MergeTree() ORDER BY $orderBy

             |""".stripMargin)
      testSpec
      } finally {
        connection.createStatement().execute(s"DROP TABLE IF EXISTS $tableName SYNC")
      }
    } match {
      case Success(_) =>
      case Failure(e) => throw e
    }
  }
}
