package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.READ_DIRECTLY_FROM_DISTRIBUTED_TABLE
import com.blackmorse.spark.clickhouse.reader.sharded.{ClickhouseShardedPartitionScan, MergeTreePartitionsPlanner}
import com.blackmorse.spark.clickhouse.reader.single.ClickhouseSinglePartitionScan
import com.blackmorse.spark.clickhouse.tables.{DistributedTable, MergeTreeTable}
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Properties

class ClickhouseScanBuilderTest extends AnyFlatSpec with Matchers {
  "For single MergeTree" should "be Single Shard strategy chosen" in {
    val clickhouseReaderConfiguration = ClickhouseReaderConfiguration(
      schema = StructType(Seq()),
      url = "",
      rowMapper = _ => Seq(),
      connectionProps = new Properties(),
      cluster = None
    )
    val table = MergeTreeTable(
      database = "db",
      name = "test_table",
      engine = "MergeTree",
      orderingKey = None
    )

    val clickhouseScanBuilder = new ClickhouseScanBuilder(clickhouseReaderConfiguration, table)

    clickhouseScanBuilder.build().isInstanceOf[ClickhouseSinglePartitionScan] should be (true)
  }

  "Distributed Table with underlying MergeTree table" should "choose MergeTree strategy" in {
    val clickhouseReaderConfiguration = ClickhouseReaderConfiguration(
      schema = StructType(Seq()),
      url = "url",
      rowMapper = _ => Seq(),
      connectionProps = new Properties(),
      cluster = None
    )
    val table = DistributedTable(
      database = "db",
      name = "clusterDistributedTestTable",
      engine = "Distributed",
      cluster = "cluster",
      underlyingTable = MergeTreeTable(
        database = "db", name = "underlying", engine = "AggregatingMergeTree", orderingKey = None
      )
    )

    val clickhouseScanBuilder = new ClickhouseScanBuilder(clickhouseReaderConfiguration, table)
    val scan = clickhouseScanBuilder.build()
    scan.isInstanceOf[ClickhouseShardedPartitionScan] should be (true)
    scan.isInstanceOf[MergeTreePartitionsPlanner] should be (true)

    scan.asInstanceOf[ClickhouseShardedPartitionScan].
      table.toString() should be ("db.underlying")
  }

  "For Distributed with read_directly_from_distributed = 1 table " should "single partition strategy applied" in {
    val properties = new Properties()
    properties.put(READ_DIRECTLY_FROM_DISTRIBUTED_TABLE, "true")
    val clickhouseReaderConfiguration = ClickhouseReaderConfiguration(
      schema = StructType(Seq()),
      url = "url",
      rowMapper = _ => Seq(),
      connectionProps = properties,
      cluster = Some("cls")
    )
    val table = DistributedTable(
      database = "db",
      name = "clusterDistributedTestTable",
      engine = "Distributed",
      cluster = "cls",
      underlyingTable = MergeTreeTable(
        database = "db", name = "name", engine = "MergeTree", orderingKey = None
      )
    )

    val clickhouseScanBuilder = new ClickhouseScanBuilder(clickhouseReaderConfiguration, table)
    val scan = clickhouseScanBuilder.build()
    scan.isInstanceOf[ClickhouseSinglePartitionScan] should be(true)

    scan.asInstanceOf[ClickhouseSinglePartitionScan]
      .table.name should be("clusterDistributedTestTable")
  }
}
