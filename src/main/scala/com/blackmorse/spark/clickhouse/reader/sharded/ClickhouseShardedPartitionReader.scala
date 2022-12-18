package com.blackmorse.spark.clickhouse.reader.sharded

import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderBase, ClickhouseReaderConfiguration}
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class ClickhouseShardedPartitionReader(chReaderConf: ClickhouseReaderConfiguration)
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val urlFunc = (hostName: String) => s"jdbc:clickhouse://$hostName"
    val url = urlFunc(partition.asInstanceOf[ShardedClickhousePartition].partitionUrl)

    val fields = chReaderConf.schema.fields.map(f => s"`${f.name}`").mkString(", ")

    val sharding = partition.asInstanceOf[ShardedClickhousePartition].limitBy.map(lb =>
      s"ORDER BY ${lb.orderingKey} LIMIT ${lb.offset}, ${lb.batchSize}")
      .getOrElse("")
    val sql = s"SELECT $fields FROM ${chReaderConf.tableInfo.name} $sharding"

    new ClickhouseReaderBase[ShardedClickhousePartition](
      chReaderConf = chReaderConf,
      connectionProvider = () =>
        new ClickHouseDriver().connect(url, chReaderConf.connectionProperties),
      sql = sql
    )
  }

}
