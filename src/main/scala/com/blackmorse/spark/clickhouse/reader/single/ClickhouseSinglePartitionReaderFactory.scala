package com.blackmorse.spark.clickhouse.reader.single

import com.blackmorse.spark.clickhouse.USE_FORCE_COLLAPSING_MODIFIER
import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderBase, ClickhouseReaderConfiguration}
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.utils.SqlUtils.prepareSqlQuery
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class ClickhouseSinglePartitionReaderFactory(chReaderConf: ClickhouseReaderConfiguration, table: ClickhouseTable) extends PartitionReaderFactory {

  private val useForceCollapsingModifier = Option(chReaderConf.connectionProps.get(USE_FORCE_COLLAPSING_MODIFIER))
    .forall(_.asInstanceOf[String].toBoolean)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val fields = chReaderConf.schema.fields.map(f => s"`${f.name}`").mkString(", ")
    val sqlQuery = prepareSqlQuery(fields, table, useForceCollapsingModifier)
    new ClickhouseReaderBase(
      chReaderConf = chReaderConf,
      connectionProvider = () => new ClickHouseDriver().connect(chReaderConf.url, chReaderConf.connectionProps),
      sql = sqlQuery
    )
  }
}
