package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.READ_DIRECTLY_FROM_DISTRIBUTED_TABLE
import com.blackmorse.spark.clickhouse.reader.sharded.{ClickhouseShardedPartitionScan, MergeTreePartitionsPlanner, PerShardPartitionsPlanner}
import com.blackmorse.spark.clickhouse.reader.single.ClickhouseSinglePartitionScan
import com.blackmorse.spark.clickhouse.tables.{ClickhouseTable, DistributedTable, MergeTreeTable}
import org.apache.spark.sql.connector.read._

class ClickhouseScanBuilder(chReaderConf: ClickhouseReaderConfiguration, clickhouseTable: ClickhouseTable) extends ScanBuilder {
  private val readDirectlyFromDistributed = Option(chReaderConf.connectionProps.get(READ_DIRECTLY_FROM_DISTRIBUTED_TABLE))
    .exists(_.asInstanceOf[String].toBoolean)

  override def build(): Scan = clickhouseTable match {
    case table: DistributedTable if readDirectlyFromDistributed =>
      new ClickhouseSinglePartitionScan(chReaderConf, table)
    case DistributedTable(database, name, _, cluster, _) if {println(s"${chReaderConf.cluster}, $cluster"); chReaderConf.cluster.exists(_ != cluster)} =>
      throw new IllegalArgumentException(s"User has pointed to the Distributed table: " +
        s"$database.$name, which is defined on the $cluster, while user" +
        s" has specified ${chReaderConf.cluster.get}")
    case DistributedTable(_, _, _, cluster, underlyingTable) =>

      underlyingTable match {
        case table: MergeTreeTable =>
          new ClickhouseShardedPartitionScan(chReaderConf.copy(cluster = Some(cluster)), table) with MergeTreePartitionsPlanner
        case _ =>
          new ClickhouseShardedPartitionScan(chReaderConf, underlyingTable) with PerShardPartitionsPlanner
      }
    case table: MergeTreeTable if chReaderConf.cluster.isDefined =>
      new ClickhouseShardedPartitionScan(chReaderConf, table) with MergeTreePartitionsPlanner
    case table => new ClickhouseSinglePartitionScan(chReaderConf, table)
  }
}