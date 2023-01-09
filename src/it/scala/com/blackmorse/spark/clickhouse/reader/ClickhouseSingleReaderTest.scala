package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClickhouseSingleReaderTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  import sqlContext.implicits._

  private def writeDuplicateData: Seq[(Int, String)] = {
    val count = 2
    val data = (1 to count).map(i => (count, i.toString))

    spark.sparkContext.parallelize(data).toDF("a", "b")
      .write
      .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

    data
  }

  private def writeDuplicateDataForCollapsingMergeTree: Seq[(Int, String, Int)] = {
    val count = 3
    val data = (1 to count)
      .map(i => (count, count.toString, if (i % 2 != 0) 1 else -1))

    spark.sparkContext.parallelize(data).toDF("a", "b", "sign")
      .write
      .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

    data
  }

  "Data" should "be collapsed from table with ReplacingMergeTree engine" in {
    withTable(Seq("a Int32", "b String"), "a", tableEngine = "ReplacingMergeTree()") {

      writeDuplicateData

      val df = spark
        .read
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.length should be(1)
    }
  }

  "Data" should "not be collapsed from table with ReplacingMergeTree engine" in {
    withTable(Seq("a Int32", "b String"), "a", tableEngine = "ReplacingMergeTree()") {

      val expectedData = writeDuplicateData

      val df = spark
        .read
        .disableForceCollapsing()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._2) should be (expectedData)
    }
  }

  "Data" should "be collapsed from table with CollapsingMergeTree engine" in {
    withTable(Seq("a Int32", "b String", "Sign Int8"), "a", tableEngine = "CollapsingMergeTree") {

      writeDuplicateDataForCollapsingMergeTree

      val df = spark
        .read
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val collect = df.collect().map(row => (row.getInt(0), row.getString(1), row.getByte(2)))

      collect.length should be(1)
    }
  }

  "Data" should "not be collapsed from table with CollapsingMergeTree engine" in {
    withTable(Seq("a Int32", "b String", "Sign Int8"), "a", tableEngine = "CollapsingMergeTree") {

      writeDuplicateDataForCollapsingMergeTree

      val df = spark
        .read
        .disableForceCollapsing()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val collect = df.collect().map(row => (row.getInt(0), row.getString(1), row.getByte(2)))

      collect.length should be(3)
    }
  }

}
