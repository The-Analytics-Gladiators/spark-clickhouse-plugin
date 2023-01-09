package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.ClickhouseTests.{withClusterTable, withTable}
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.Partitioner
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SimpleShardedWriterTests extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  it should "write to standalone table" in {
    withTable(Seq("a Int32", "b String"), "a") {
      import sqlContext.implicits._

      val data = (1 to 20) map (i => (i, i.toString))

      val rdd = spark.sparkContext.parallelize(data)
      rdd.toDF("a", "b")
        .write
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val result = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      result.sortBy(_._1) shouldEqual data
    }
  }

  it should "write to MergeTree on cluster" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = false) {
      import sqlContext.implicits._

      val data = (1 to 20) map (i => (i, i.toString))

      spark.sparkContext.parallelize(data).partitionBy(new Partitioner {
        override def numPartitions: Int = 2
        override def getPartition(key: Any): Int = if(key.asInstanceOf[Int] <= 10) 0 else 1
      })
        .toDF("a", "b")
        .write
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterTestTable)

      val shard1Result = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      val shard2Result = spark.read.clickhouse(shard2Replica1.hostName, shard2Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      shard1Result.sortBy(_._1) shouldEqual (1 to 10).map(i => (i, i.toString))
      shard2Result.sortBy(_._1) shouldEqual (11 to 20).map(i => (i, i.toString))
    }
  }

  it should "write to Distributed underlying table" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      import sqlContext.implicits._

      val data = (1 to 20) map (i => (i, i.toString))
      spark.sparkContext.parallelize(data).partitionBy(new Partitioner {
        override def numPartitions: Int = 2
        override def getPartition(key: Any): Int = if (key.asInstanceOf[Int] <= 10) 0 else 1
      })
        .toDF("a", "b")
        .write
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)

      val shard1Result = spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      val shard2Result = spark.read.clickhouse(shard2Replica1.hostName, shard2Replica1.port, clusterTestTable)
        .collect().map(row => (row.getInt(0), row.getString(1)))

      shard1Result.sortBy(_._1) shouldEqual (1 to 10).map(i => (i, i.toString))
      shard2Result.sortBy(_._1) shouldEqual (11 to 20).map(i => (i, i.toString))
    }
  }

  it should "fail when user's cluster does not match Distributed table cluster" in {
    withClusterTable(Seq("a Int32"), "a", withDistributed = true) {
      assertThrows[Exception] {
        spark.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, "wrong_cluster", clusterDistributedTestTable)
          .collect()
      }
    }
  }

  it should "write directly to Distributed table" in {
    withClusterTable(Seq("a Int32", "b String"), "a", withDistributed = true) {
      import sqlContext.implicits._

      val data = (1 to 20) map (i => (i, i.toString))
      // Error when writing to Distributed table without specified sharing key
      // Mean that we are actually trying to push data to Distributed table, not to the underlying one
      assertThrows[Exception] {
        spark.sparkContext.parallelize(data)
          .toDF("a", "b")
          .write
          .writeDirectlyToDistributedTable()
          .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterDistributedTestTable)
      }
    }
  }
}
