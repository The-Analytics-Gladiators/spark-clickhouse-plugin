package com.blackmorse.spark.clickhouse.write

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable

class SingleFieldsTests extends AnyFlatSpec with DataFrameSuiteBase {
  import com.blackmorse.spark.clickhouse._
  import sqlContext.implicits._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  "Primitive type Int32" should "be written and read" in {
    withTable(Seq("a Int32"), "a") {
      val seq = 1 to 100
      sc.parallelize(seq).toDF("a")
        .write.clickhouse(host, port, table)

      val res = sqlContext.read.clickhouse(host, port, table).rdd
        .map(_.getInt(0)).collect()

      assert(res.sorted sameElements seq)
    }
  }

}
