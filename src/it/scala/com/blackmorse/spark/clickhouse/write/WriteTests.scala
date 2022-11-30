package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import com.blackmorse.spark.clickhouse.ClickhouseHosts._

class WriteTests extends AnyFlatSpec with DataFrameSuiteBase {
  import com.blackmorse.spark.clickhouse._
  import sqlContext.implicits._

  "Plugin " should "write Int32 to MergeTree table" in {
    withTable(Seq("a Int32"), "a") {
      val df = sc.parallelize(Seq(1, 2, 3))
        .toDF("a")

      df.write.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val resDf = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val result = resDf.collect().map(_.getInt(0)).sorted

      assert(result sameElements Array(1, 2, 3))
    }
  }

  "Two fields" should "be written into 2 corresponding fields" in {
    withTable(Seq("a String", "b Int32"), "a") {
      val seq = Seq(("1", 1), ("2", 2))
      sc.parallelize(seq).toDF("a", "b")
        .write.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val result = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)
        .map(row => (row.getString(0), row.getInt(1))).collect().sorted

      assert(result sameElements seq)
    }
  }

  //BY default datasourcev2 can't do this
  "Two fields" should "be written into table with 3 fields" ignore {
    withTable(Seq("a Int16", "b Int64", "c String"), "a") {
      val seq = Seq((1.toShort, 1L), (2.toShort, 2L))
      sc.parallelize(seq).toDF("a", "b")
        .write.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val result = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)
        .map(row => (row.getShort(0), row.getLong(1), row.getString(2)))
        .collect()
        .sortBy(_._1)

      val expected = seq.map { case (a, b) => (a, b, "") }
      assert(result sameElements expected)
    }
  }
}
