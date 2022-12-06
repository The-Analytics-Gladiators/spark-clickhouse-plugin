package com.blackmorse.spark.clickhouse.reader.single

import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderBase, ClickhouseReaderConfiguration}
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class ClickhouseSinglePartitionReaderFactory(clickhouseReaderInfo: ClickhouseReaderConfiguration) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new ClickhouseReaderBase(clickhouseReaderInfo = clickhouseReaderInfo,
      connectionProvider = () => new ClickHouseDriver().connect(clickhouseReaderInfo.url, clickhouseReaderInfo.connectionProperties)
    )
}
