package com.blackmorse.spark.clickhouse.cluster

import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.ClickhouseTests.withClusterTable
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Encoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spire.ClassTag

class ClusterShardedReaderTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  import sqlContext.implicits._

  def writeData[T:ClassTag](shard1Data: Seq[T], shard2Data: Seq[T], columns: Seq[String] = Seq("a", "b"))
                           (implicit e: Encoder[T]): Seq[T] = {
    spark.sparkContext.parallelize[T](shard1Data).toDF(columns:_*)
      .write
      .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterTestTable)

    spark.sparkContext.parallelize[T](shard2Data).toDF(columns:_*)
      .write
      .clickhouse(shard2Replica1.hostName, shard2Replica1.port, clusterTestTable)

    shard1Data ++ shard2Data
  }

  private def generateData(count: Int): (Seq[(Int, String)], Seq[(Int, String)]) = {
    val shard1Data = (1 to count).map(i => (i, i.toString))
    val shard2Data = ((count + 1) to (count * 2)).map(i => (i, i.toString))
    (shard1Data, shard2Data)
  }

  private def generateDuplicateData(): (Seq[(Int, String)], Seq[(Int, String)]) = {
    val count = 2
    val shard1Data = (1 to count).map(i => (count, i.toString))
    val shard2Data = ((count + 1) to (count * 2)).map(i => (count, i.toString))
    (shard1Data, shard2Data)
  }

  private def generateDuplicateDataForCollapsingMergeTree(): Seq[(Int, String, Int)] = {
    val count = 3
    (1 to count).map(i => (count, count.toString, if (i % 2 != 0) 1 else -1))
  }

  "Data" should "be collapsed from distributed table with ReplacingMergeTree engine" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true,
      tableEngine = "ReplicatedReplacingMergeTree") {

      val (shard1Data, shard2Data) = generateDuplicateData()
      writeData(shard1Data, shard2Data)

      val df = spark
        .read
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterDistributedTestTable)

      df.rdd.partitions.length should be(2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.length should be(2)
    }
  }

  "Data" should "not be collapsed from distributed table with ReplacingMergeTree engine" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true,
      tableEngine = "ReplicatedReplacingMergeTree") {

      val (shard1Data, shard2Data) = generateDuplicateData()
      val expectedData = writeData(shard1Data, shard2Data)

      val df = spark
        .read
        .disableForceCollapsing()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterDistributedTestTable)

      df.rdd.partitions.length should be(2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.length should be(4)
      collect.sortBy(_._2) should be (expectedData)
    }
  }

  "Data" should "be collapsed from distributed table with CollapsingMergeTree engine" in {
    withClusterTable(Seq("a Int32", "b String", "Sign Int8"), "a", withDistributed = true,
      tableEngine = "ReplicatedCollapsingMergeTree") {

      val data = generateDuplicateDataForCollapsingMergeTree()
      writeData(data, data, Seq("a", "b", "Sign"))

      val df = spark
        .read
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterDistributedTestTable)

      df.rdd.partitions.length should be(2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1), row.getByte(2)))

      collect.length should be(1)
    }
  }

  "Data" should "not be collapsed from distributed table with CollapsingMergeTree engine" in {
    withClusterTable(Seq("a Int32", "b String", "Sign Int8"), "a", withDistributed = true,
      tableEngine = "ReplicatedCollapsingMergeTree") {

      val data = generateDuplicateDataForCollapsingMergeTree()
      writeData(data, data, Seq("a", "b", "Sign"))

      val df = spark
        .read
        .disableForceCollapsing()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterDistributedTestTable)

      df.rdd.partitions.length should be(2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1), row.getByte(2)))

      collect.length should be(4)
    }
  }

  it should "read from sharded MergeTree table" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = false) {
      val (shard1Data, shard2Data) = generateData(2)
      val writtenData = writeData(shard1Data, shard2Data)

      val df = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterTestTable)

      df.rdd.partitions.length should be (2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "read from distributed table with specified cluster" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      val (shard1Data, shard2Data) = generateData(2)
      val writtenData = writeData(shard1Data, shard2Data)

      val df = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterDistributedTestTable)

      df.rdd.partitions.length should be (2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "read from distributed table without specifying cluster" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      val (shard1Data, shard2Data) = generateData(2)
      val writtenData = writeData(shard1Data, shard2Data)

      val df = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      df.rdd.partitions.length should be(2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "read from distributed table even with restriction of reading the underlying table" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      val (shard1Data, shard2Data) = generateData(2)
      val writtenData = writeData(shard1Data, shard2Data)

      val df = spark.read
        .readDirectlyFromDistributedTable()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      df.rdd.partitions.length should be(1)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "fail on non-matching clusters" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      assertThrows[Exception] {
        spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, "wrong_cluster", clusterDistributedTestTable)
          .collect()
      }
    }
  }

  it should "read from underlying MergeTree table by batches" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      val (shard1Data, shard2Data) = generateData(9)
      val writtenData = writeData(shard1Data, shard2Data)

      val df = spark.read
        .batchSize(2)
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      df.rdd.partitions.length should be (10)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "read from underlying MergeTree table with no ordering key by batches" in {
    withClusterTable(Seq("a Int32", "b String"), "tuple()", withDistributed = true) {
      val (shard1Data, shard2Data) = generateData(9)
      val writtenData = writeData(shard1Data, shard2Data)

      val df = spark.read
        .batchSize(2)
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      df.rdd.partitions.length should be(10)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be(writtenData)
    }
  }
}
