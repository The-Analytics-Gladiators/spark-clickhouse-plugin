package com.blackmorse.spark.clickhouse.write

import com.clickhouse.jdbc.ClickHouseDriver
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import com.blackmorse.spark.clickhouse._

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
      .clickhouse("localhost", 8123, "default.t")

    val resDf = sqlContext.read
      .clickhouse("localhost", 8123, "default.t")

    val result = resDf.collect().map(_.getInt(0)).sorted
    assert(result sameElements Array(1, 2, 3))

    connection.createStatement().execute("DROP TABLE t")
  }
}
