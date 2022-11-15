package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.spark.types.ClickhouseSchemaParser
import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = getReaderInfo(options).schema

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new ClickhouseTable(getReaderInfo(properties))

  private def getReaderInfo(options: util.Map[String, String]): ClickhouseReaderConfiguration = {
    val hostName = options.get(CLICKHOUSE_HOST_NAME)
    val port = options.getOrDefault(CLICKHOUSE_PORT, "8123")
    val table = options.get(TABLE)
    val url = s"jdbc:clickhouse://$hostName:$port"

    val clickhouseFields = ClickhouseSchemaParser.parseTable(url, table)
    val clickhouseTimeZoneInfo = JDBCTimeZoneUtils.fetchClickhouseTimeZoneFromServer(url)

    val schema = StructType(clickhouseFields.map(clickhouseField => clickhouseField.typ.toSparkType match {
      case ArrayType(elementType, _) => StructField(clickhouseField.name, ArrayType(elementType, true))
      case _ => StructField(clickhouseField.name, clickhouseField.typ.toSparkType, /*clickhouse.typ.nullable*/true)
    })
    )

    reader.ClickhouseReaderConfiguration(
      schema = schema,
      tableName = table,
      url = url,
      rowMapper = rs => clickhouseFields.map(_.extractFromRs(rs)(clickhouseTimeZoneInfo))
    )
  }
}
