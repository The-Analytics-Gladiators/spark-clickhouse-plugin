package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import com.blackmorse.spark.clickhouse._

class WriteTests extends AnyFlatSpec with DataFrameSuiteBase {
  "Plugin " should "write Int32 to MergeTree table" in {
    withTable("default.t", Seq("a Int32"), "a") {

      import sqlContext.implicits._
      val df = sc.parallelize(Seq(1, 2, 3))
        .toDF()
        .withColumnRenamed("value", "a")

      df.write
        .clickhouse("localhost", 8123, "default.t")

      val resDf = sqlContext.read
        .clickhouse("localhost", 8123, "default.t")

      val result = resDf.collect().map(_.getInt(0)).sorted
      println(result.toSeq)
      assert(result sameElements Array(1, 2, 3))
    }
  }
}
