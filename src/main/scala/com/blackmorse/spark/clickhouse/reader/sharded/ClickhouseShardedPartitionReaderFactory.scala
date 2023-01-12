package com.blackmorse.spark.clickhouse.reader.sharded

import com.blackmorse.spark.clickhouse.FORCE_COLLAPSING_MODIFIER
import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderBase, ClickhouseReaderConfiguration}
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.utils.SqlUtils.prepareSqlQuery
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class ClickhouseShardedPartitionReaderFactory(chReaderConf: ClickhouseReaderConfiguration, table: ClickhouseTable)
    extends PartitionReaderFactory {

  private val useForceCollapsingModifier = Option(chReaderConf.connectionProps.get(FORCE_COLLAPSING_MODIFIER))
    .forall(_.asInstanceOf[String].toBoolean)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val urlFunc = (hostName: String) => s"jdbc:clickhouse://$hostName"
    val url = urlFunc(partition.asInstanceOf[ShardedClickhousePartition].partitionUrl)

    val fields = chReaderConf.schema.fields.map(f => s"`${f.name}`").mkString(", ")

    val sharding = partition.asInstanceOf[ShardedClickhousePartition].limitBy.map(lb => {
      val orderBy = lb.orderingKey.map(ok => s"ORDER BY $ok").getOrElse("")
      s"$orderBy LIMIT ${lb.offset}, ${lb.batchSize}"
    }).getOrElse("")

    val sqlQuery = prepareSqlQuery(fields, table, useForceCollapsingModifier)
    val finalSqlQuery = s"$sqlQuery $sharding"

    new ClickhouseReaderBase[ShardedClickhousePartition](
      chReaderConf = chReaderConf,
      connectionProvider = () =>
        new ClickHouseDriver().connect(url, chReaderConf.connectionProps),
      sql = finalSqlQuery
    )
  }
}
