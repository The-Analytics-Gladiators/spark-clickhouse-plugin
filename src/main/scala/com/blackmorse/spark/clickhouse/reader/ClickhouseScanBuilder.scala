package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.reader.sharded.ClickhouseShardedPartitionScan
import com.blackmorse.spark.clickhouse.reader.single.ClickhouseSinglePartitionScan
import org.apache.spark.sql.connector.read._

class ClickhouseScanBuilder(clickhouseReaderInfo: ClickhouseReaderConfiguration) extends ScanBuilder {
  override def build(): Scan = clickhouseReaderInfo.cluster match {
    case Some(_) => new ClickhouseShardedPartitionScan(clickhouseReaderInfo)
    case None => new ClickhouseSinglePartitionScan(clickhouseReaderInfo)
  }
}