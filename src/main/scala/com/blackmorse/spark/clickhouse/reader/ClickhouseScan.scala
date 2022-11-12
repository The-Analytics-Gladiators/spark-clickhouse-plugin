package com.blackmorse.spark.clickhouse.reader

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType

class ClickhouseScanBuilder(clickhouseReaderInfo: ClickhouseReaderInfo) extends ScanBuilder {
  override def build(): Scan = new ClickhouseScan(clickhouseReaderInfo)
}

class ClickhouseScan(clickhouseReaderInfo: ClickhouseReaderInfo) extends Scan with Batch {
  //Scaling strategy will be applied here
  override def planInputPartitions(): Array[InputPartition] =
    Array(ClickhouseInputPartition())

  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory =
    new ClickhousePartitionReaderFactory(clickhouseReaderInfo)

  override def readSchema(): StructType = clickhouseReaderInfo.schema
}
