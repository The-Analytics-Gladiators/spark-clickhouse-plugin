package com.blackmorse.spark.clickhouse.reader.single

import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class ClickhouseSinglePartitionScan(val clickhouseReaderInfo: ClickhouseReaderConfiguration, val table: ClickhouseTable) extends Scan with Batch {
  //Scaling strategy will be applied here
  private val SINGLE_PARTITION: Array[InputPartition] = Array(ClickhouseSingleInputPartition())
  override def planInputPartitions(): Array[InputPartition] = SINGLE_PARTITION

  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory =
    new ClickhouseSinglePartitionReaderFactory(clickhouseReaderInfo, table)

  override def readSchema(): StructType = clickhouseReaderInfo.schema
}
