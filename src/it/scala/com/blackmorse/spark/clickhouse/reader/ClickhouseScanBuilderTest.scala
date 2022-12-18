package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.ClickhouseTests.withClusterTable
import com.blackmorse.spark.clickhouse.reader.single.ClickhouseSinglePartitionScan
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.READ_DIRECTLY_FROM_DISTRIBUTED_TABLE
import com.blackmorse.spark.clickhouse.reader.sharded.ClickhouseShardedPartitionScan
import com.blackmorse.spark.clickhouse.reader.sharded.mergetree.MergeTreePartitionScan

import java.util.Properties

class ClickhouseScanBuilderTest extends AnyFlatSpec with Matchers {
  "For single MergeTree" should "be Single Shard strategy chosen" in {
    val clickhouseReaderConfiguration = ClickhouseReaderConfiguration(
      schema = StructType(Seq()),
      url = "",
      tableInfo = TableInfo(
        name = "test_table",
        engine = "MergeTree",
        cluster = None,
        orderingKey = None
      ),
      rowMapper = _ => Seq(),
      connectionProperties = new Properties()
    )

    val clickhouseScanBuilder = new ClickhouseScanBuilder(clickhouseReaderConfiguration)

    clickhouseScanBuilder.build().isInstanceOf[ClickhouseSinglePartitionScan] should be (true)
  }

  "Distributed Table with underlying MergeTree table" should "choose MergeTree strategy" in {
    withClusterTable(Seq("a UInt32"), "a", withDistributed = true) {
      val clickhouseReaderConfiguration = ClickhouseReaderConfiguration(
        schema = StructType(Seq()),
        url = s"jdbc:clickhouse://${shard1Replica1.hostName}:${shard1Replica1.port}",
        tableInfo = TableInfo(
          name = clusterDistributedTestTable,
          engine = "Distributed",
          cluster = None,
          orderingKey = None
        ),
        rowMapper = _ => Seq(),
        connectionProperties = new Properties()
      )

      val clickhouseScanBuilder = new ClickhouseScanBuilder(clickhouseReaderConfiguration)
      val scan = clickhouseScanBuilder.build()
      scan.isInstanceOf[MergeTreePartitionScan] should be (true)

      scan.asInstanceOf[MergeTreePartitionScan].chReaderConf
        .tableInfo.name should be (clusterTestTable)
    }
  }

  "For Distributed with read_directly_from_distributed = 1 table " should "single partition strategy applied" in {
    withClusterTable(Seq("a UInt32"), "a", withDistributed = true) {
      val properties = new Properties()
      properties.put(READ_DIRECTLY_FROM_DISTRIBUTED_TABLE, "true")
      val clickhouseReaderConfiguration = ClickhouseReaderConfiguration(
        schema = StructType(Seq()),
        url = s"jdbc:clickhouse://${shard1Replica1.hostName}:${shard1Replica1.port}",
        tableInfo = TableInfo(
          name = clusterDistributedTestTable,
          engine = "Distributed",
          cluster = None,
          orderingKey = None
        ),
        rowMapper = _ => Seq(),
        connectionProperties = properties
      )

      val clickhouseScanBuilder = new ClickhouseScanBuilder(clickhouseReaderConfiguration)
      val scan = clickhouseScanBuilder.build()
      scan.isInstanceOf[ClickhouseSinglePartitionScan] should be(true)

      scan.asInstanceOf[ClickhouseSinglePartitionScan].clickhouseReaderInfo
        .tableInfo.name should be(clusterDistributedTestTable)
    }
  }
}
