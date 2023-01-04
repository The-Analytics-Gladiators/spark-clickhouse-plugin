package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseHosts.{clusterDistributedTestTable, clusterName, clusterTestTable, shard1Replica1, shard2Replica1}
import com.blackmorse.spark.clickhouse.ClickhouseTests.withClusterTable
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RandomShardingTests extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  it should "write to MergeTree on cluster" in {
    withClusterTable(Seq("a Int32", "b String"), "a", false) {
      import sqlContext.implicits._

      val data = (1 to 100) map (i => (i, i.toString))

      val rdd = spark.sparkContext.parallelize(data)
      rdd.toDF("a", "b")
        .write
        .shuffle()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterTestTable)

      val result1 = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      val result2 = spark.read.clickhouse(shard2Replica1.hostName, shard2Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      (result1 ++ result2).sortBy(_._1) shouldEqual data
      // Hope random will bless us
      result1.isEmpty shouldBe false
      result2.isEmpty shouldBe false
    }
  }

  it should "write to Distributed table over MergeTree" in {
    withClusterTable(Seq("a Int32", "b String"), "a", true) {
      import sqlContext.implicits._

      val data = (1 to 100) map (i => (i, i.toString))

      val rdd = spark.sparkContext.parallelize(data)
      rdd.toDF("a", "b")
        .write
        .shuffle()
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      val result1 = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      val result2 = spark.read.clickhouse(shard2Replica1.hostName, shard2Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      (result1 ++ result2).sortBy(_._1) shouldEqual data
      // The same
      result1.isEmpty shouldBe false
      result2.isEmpty shouldBe false
    }
  }

  it should "write when splitted by batches" in {
    withClusterTable(Seq("a Int32", "b String"), "a", true) {
      import sqlContext.implicits._

      val data = (1 to 100) map (i => (i, i.toString))

      val rdd = spark.sparkContext.parallelize(data)
      rdd.toDF("a", "b")
        .write
        .shuffle()
        .batchSize(1000)
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      val result = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      result.sortBy(_._1) shouldEqual data
    }
  }
}
