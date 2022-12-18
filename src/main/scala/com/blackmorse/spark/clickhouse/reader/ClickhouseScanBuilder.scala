package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.READ_DIRECTLY_FROM_DISTRIBUTED_TABLE
import com.blackmorse.spark.clickhouse.reader.sharded.ClickhouseShardedPartitionScan
import com.blackmorse.spark.clickhouse.reader.sharded.mergetree.MergeTreePartitionScan
import com.blackmorse.spark.clickhouse.reader.single.ClickhouseSinglePartitionScan
import com.blackmorse.spark.clickhouse.services.{ClickhouseDistributedTableService, ClickhouseTableService}
import org.apache.spark.sql.connector.read._

class ClickhouseScanBuilder(chReaderConf: ClickhouseReaderConfiguration) extends ScanBuilder {

  private val readDirectlyFromDistributed = Option(chReaderConf.connectionProperties.get(READ_DIRECTLY_FROM_DISTRIBUTED_TABLE))
    .map(_.asInstanceOf[String].toBoolean)
    .getOrElse(false)

  override def build(): Scan = chReaderConf.tableInfo.engine match {
    case "Distributed" if readDirectlyFromDistributed =>
      new ClickhouseSinglePartitionScan(chReaderConf)
    case "Distributed" =>
      val underlyingTable = ClickhouseTableService.getUnderlyingTableForDistributed(chReaderConf)
      if (ClickhouseDistributedTableService.isMergeTree(underlyingTable)) {
        new MergeTreePartitionScan(chReaderConf.copy(tableInfo = underlyingTable))
      } else {
        new ClickhouseShardedPartitionScan(chReaderConf.copy(tableInfo = underlyingTable))
      }
    case _ => chReaderConf.tableInfo.cluster match {
      case Some(cluster) =>
        val tableInfo = ClickhouseTableService.getTableInfo(chReaderConf.url, chReaderConf.tableInfo.name, cluster, chReaderConf.connectionProperties).get
        if (ClickhouseDistributedTableService.isMergeTree(tableInfo)) {
           new MergeTreePartitionScan(chReaderConf.copy(tableInfo = tableInfo))
        } else {
          new ClickhouseShardedPartitionScan(chReaderConf)
        }
      case None => new ClickhouseSinglePartitionScan(chReaderConf)
    }
  }
}