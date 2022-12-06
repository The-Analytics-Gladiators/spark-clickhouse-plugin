package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class ArrayFieldTests extends AnyFlatSpec with DataFrameSuiteBase {
  import com.blackmorse.spark.clickhouse._
  import sqlContext.implicits._
  import collection.JavaConverters._
  val host = "clickhouse-server"
  val port = 8123
  val table = "default.test_table"

  "Array(Int8)" should "be written and read" in {
    withTable(Seq("a Array(Int8)"), "a") {
      val seq = Seq(Seq(1, 2, 3), Seq())
      sc.parallelize(seq)
        .toDF("a")
        .write.clickhouse(host, port, table)

       val res = sqlContext.read.clickhouse(host, port, table)
         .rdd
         .map(row => row.getList[Byte](0))
         .collect()

      res.sortBy(_.size()).zip(seq.sortBy(_.size)).foreach{ case (l, r) =>
        assert(l.asScala == r)
      }
    }
  }
}
