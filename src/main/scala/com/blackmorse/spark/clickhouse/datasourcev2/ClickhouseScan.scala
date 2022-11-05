package com.blackmorse.spark.clickhouse.datasourcev2

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, ScanBuilder}
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
