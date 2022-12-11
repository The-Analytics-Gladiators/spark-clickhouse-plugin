package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.READ_DIRECTLY_FROM_DISTRIBUTED_TABLE
import com.blackmorse.spark.clickhouse.parsers.ClickhouseDistributedEngineParser
import com.blackmorse.spark.clickhouse.reader.sharded.ClickhouseShardedPartitionScan
import com.blackmorse.spark.clickhouse.reader.single.ClickhouseSinglePartitionScan
import org.apache.spark.sql.connector.read._

class ClickhouseScanBuilder(clickhouseReaderInfo: ClickhouseReaderConfiguration) extends ScanBuilder {
  private val readDirectlyFromDistributed = Option(clickhouseReaderInfo.connectionProperties.get(READ_DIRECTLY_FROM_DISTRIBUTED_TABLE))
    .map(_.asInstanceOf[String].toBoolean)
    .getOrElse(false)

  override def build(): Scan = clickhouseReaderInfo.tableInfo.engine match {
    case "Distributed" if readDirectlyFromDistributed =>
      new ClickhouseSinglePartitionScan(clickhouseReaderInfo)
    case "Distributed" =>
      val underlyingTable = ClickhouseDistributedEngineParser.getUnderlyingTable(clickhouseReaderInfo)
      new ClickhouseShardedPartitionScan(clickhouseReaderInfo.copy(tableInfo = underlyingTable))
    case _ => clickhouseReaderInfo.tableInfo.cluster match {
      case Some(_) => new ClickhouseShardedPartitionScan(clickhouseReaderInfo)
      case None => new ClickhouseSinglePartitionScan(clickhouseReaderInfo)
    }
  }
}