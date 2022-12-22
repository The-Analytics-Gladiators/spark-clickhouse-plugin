package com.blackmorse.spark.clickhouse.cluster

import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.ClickhouseTests.withClusterTable
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClusterShardedReaderTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  private def writeData(count: Int): Seq[(Int, String)] = {
    import sqlContext.implicits._
    val shardData1 = (1 to count).map(i => (i, i.toString))
    spark.sparkContext.parallelize(shardData1).toDF("a", "b")
      .write
      .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterTestTable)

    val shardData2 = ((count + 1) to (count * 2)).map(i => (i, i.toString))
    spark.sparkContext.parallelize(shardData2).toDF("a", "b")
      .write.clickhouse(shard2Replica1.hostName, shard2Replica1.port, clusterTestTable)

    shardData1 ++ shardData2
  }

  it should "read from sharded MergeTree table" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = false) {
      val writtenData = writeData(2)

      val df = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterTestTable)

      df.rdd.partitions.length should be (2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "read from distributed table with specified cluster" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      val writtenData = writeData(2)

      val df = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterDistributedTestTable)

      df.rdd.partitions.length should be (2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "read from distributed table without specifying cluster" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      val writtenData = writeData(2)

      val df = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      df.rdd.partitions.length should be(2)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be (writtenData)
    }
  }

  it should "read from distributed table even with restriction of reading the underlying table" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      val writtenData = writeData(2)

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
      val writtenData = writeData(9)
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
      val writtenData = writeData(9)
      val df = spark.read
        .batchSize(2)
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      df.rdd.partitions.length should be(10)
      val collect = df.collect().map(row => (row.getInt(0), row.getString(1)))

      collect.sortBy(_._1) should be(writtenData)
    }
  }
}
