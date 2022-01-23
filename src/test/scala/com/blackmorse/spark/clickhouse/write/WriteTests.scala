package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.{CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE}
import com.clickhouse.jdbc.ClickHouseDriver
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Properties

class WriteTests extends AnyFlatSpec with DataFrameSuiteBase {
  "Plugin " should "write Int32 to MergeTree table" in {
    val connection = new ClickHouseDriver().connect("jdbc:clickhouse://localhost:8123", new Properties())
    connection
      .createStatement().execute(
      """
        |CREATE TABLE default.t (
        |   a Int32
        |) ENGINE = MergeTree() ORDER BY a
        |""".stripMargin)

    import sqlContext.implicits._
    val df = sc.parallelize(Seq(1, 2, 3))
      .toDF()
      .withColumnRenamed("value", "a")

    df.write
      .format("com.blackmorse.spark.clickhouse")
      .option(CLICKHOUSE_HOST_NAME, "localhost")
      .option(CLICKHOUSE_PORT, "8123")
      .option(TABLE, "default.t")
      .save()

    val rs = connection.createStatement().executeQuery("SELECT * FROM default.t ORDER BY a")

    rs.next()
    assert(rs.getInt(1) == 1)
    rs.next()
    assert(rs.getInt(1) == 2)
    rs.next()
    assert(rs.getInt(1) == 3)

    connection.createStatement().execute("DROP TABLE t")
  }
}
