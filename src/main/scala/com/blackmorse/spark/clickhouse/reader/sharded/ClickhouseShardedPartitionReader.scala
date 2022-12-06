package com.blackmorse.spark.clickhouse.reader.sharded

import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderBase, ClickhouseReaderConfiguration}
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class ClickhouseShardedPartitionReader(clickhouseReaderConfiguration: ClickhouseReaderConfiguration)
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val urlFunc = (hostName: String) => s"jdbc:clickhouse://$hostName"
    val url = urlFunc(partition.asInstanceOf[ShardedClickhousePartition].partitionUrl)

    new ClickhouseReaderBase[ShardedClickhousePartition](clickhouseReaderInfo = clickhouseReaderConfiguration,
      connectionProvider = () =>
        new ClickHouseDriver().connect(url, clickhouseReaderConfiguration.connectionProperties))
  }

}
