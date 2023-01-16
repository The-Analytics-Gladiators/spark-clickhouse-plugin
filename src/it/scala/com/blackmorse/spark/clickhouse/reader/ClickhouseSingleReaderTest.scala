package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Encoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

class ClickhouseSingleReaderTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  import sqlContext.implicits._

  def writeData[T: ClassTag](data: Seq[T], columns: Seq[String] = Seq("a", "b"))(implicit e: Encoder[T]): Unit = {
    spark.sparkContext.parallelize[T](data).toDF(columns: _*)
      .write
      .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)
  }

  private def generateDuplicateData(): Seq[(Int, String)] = {
    val count = 2
    (1 to count).map(i => (count, i.toString))
  }

  private def generateDuplicateDataForCollapsingMergeTree(): Seq[(Int, String, Int)] = {
    val count = 3
    (1 to count).map(i =>
      (count, if (i == count) (count * 2).toString else count.toString, if (i % 2 != 0) 1 else -1))
  }

  "Data" should "be collapsed from table with ReplacingMergeTree engine" in {
    withTable(Seq("a Int32", "b String"), "a", tableEngine = "ReplacingMergeTree()") {

      val data = generateDuplicateData()
      writeData(data)

      val df = spark
        .read
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.length should be(1)
    }
  }

  "Data" should "not be collapsed from table with ReplacingMergeTree engine" in {
    withTable(Seq("a Int32", "b String"), "a", tableEngine = "ReplacingMergeTree()") {

      val expectedData = generateDuplicateData()
      writeData(expectedData)

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

      val data = generateDuplicateDataForCollapsingMergeTree()
      writeData(data, Seq("a", "b", "Sign"))

      val df = spark
        .read
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val collect = df.collect().map(row => (row.getInt(0), row.getString(1), row.getByte(2)))

      collect.length should be(1)
    }
  }

  "Data" should "not be collapsed from table with CollapsingMergeTree engine" in {
    withTable(Seq("a Int32", "b String", "Sign Int8"), "a", tableEngine = "CollapsingMergeTree") {

      val data = generateDuplicateDataForCollapsingMergeTree()
      writeData(data, Seq("a", "b", "Sign"))

      val df = spark
        .read
        .disableForceCollapsing()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val collect = df.collect().map(row => (row.getInt(0), row.getString(1), row.getByte(2)))

      collect.length should be(3)
    }
  }

}
