package com.blackmorse.spark.clickhouse.reader

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType

class ClickhouseScanBuilder(clickhouseReaderInfo: ClickhouseReaderConfiguration) extends ScanBuilder {
  override def build(): Scan = new ClickhouseScan(clickhouseReaderInfo)
}

class ClickhouseScan(clickhouseReaderInfo: ClickhouseReaderConfiguration) extends Scan with Batch {
  //Scaling strategy will be applied here
  private val SINGLE_PARTITION: Array[InputPartition] = Array(ClickhouseInputPartition())
  override def planInputPartitions(): Array[InputPartition] = SINGLE_PARTITION

  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory =
    new ClickhousePartitionReaderFactory(clickhouseReaderInfo)

  override def readSchema(): StructType = clickhouseReaderInfo.schema
}
