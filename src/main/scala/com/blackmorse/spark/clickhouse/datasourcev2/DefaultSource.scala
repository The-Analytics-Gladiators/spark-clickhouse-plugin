package com.blackmorse.spark.clickhouse.datasourcev2

import com.blackmorse.spark.clickhouse.reader.ClickhouseSchemaParser
import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.{CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = getReaderInfo(options).schema

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new ClickhouseTable(getReaderInfo(properties))

  private def getReaderInfo(options: util.Map[String, String]): ClickhouseReaderInfo = {
    val hostName = options.get(CLICKHOUSE_HOST_NAME)
    val port = options.get(CLICKHOUSE_PORT)
    val table = options.get(TABLE)

    val url = s"jdbc:clickhouse://$hostName:$port"

    val clickhouseFields = ClickhouseSchemaParser.parseTable(url, table)
    val clickhouseTimeZoneInfo = JDBCTimeZoneUtils.fetchClickhouseTimeZoneFromServer(url)

    val schema = StructType(clickhouseFields.map(clickhouseField => StructField(clickhouseField.name, clickhouseField.typ.toSparkType(), clickhouseField.typ.nullable)))
    ClickhouseReaderInfo(
      schema = schema,
      tableName = table,
      url = url,
      rowMapper = rs => clickhouseFields.map(_.extractFromRs(rs)(clickhouseTimeZoneInfo))
    )
  }
}
