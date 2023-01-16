package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.DIRECTLY_USE_DISTRIBUTED_TABLE
import com.blackmorse.spark.clickhouse.reader.sharded.{ClickhouseShardedPartitionScan, MergeTreePartitionsPlanner, PerShardPartitionsPlanner}
import com.blackmorse.spark.clickhouse.reader.single.ClickhouseSinglePartitionScan
import com.blackmorse.spark.clickhouse.tables.{ClickhouseTable, DistributedTable, MergeTreeTable}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter


class ClickhouseScanBuilder(chReaderConf: ClickhouseReaderConfiguration, clickhouseTable: ClickhouseTable)
    extends ScanBuilder
    with SupportsPushDownFilters {
  private var filters: Array[Filter] = Array.empty

  private val directlyUseDistributed = Option(chReaderConf.connectionProps.get(DIRECTLY_USE_DISTRIBUTED_TABLE))
    .map(_.asInstanceOf[String].toBoolean)
    .getOrElse(false)

  override def build(): Scan = clickhouseTable match {
    case table: DistributedTable if directlyUseDistributed =>
      new ClickhouseSinglePartitionScan(chReaderConf, table)
    case DistributedTable(database, name, _, cluster, _) if chReaderConf.cluster.exists(_ != cluster) =>
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

  //TODO
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    filters
  }

  override def pushedFilters(): Array[Filter] = filters
}