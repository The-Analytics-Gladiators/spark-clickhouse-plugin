package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderConfiguration, TableInfo}
import com.blackmorse.spark.clickhouse.services.ClickhouseTableService
import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import org.apache.commons.collections.MapUtils
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.util.{Failure, Success}

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = getReaderInfo(options).schema

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new ClickhouseTable(getReaderInfo(properties))

  private def getReaderInfo(options: util.Map[String, String]): ClickhouseReaderConfiguration = {
    val hostName = options.get(CLICKHOUSE_HOST_NAME)
    val port = options.getOrDefault(CLICKHOUSE_PORT, "8123")
    val table = options.get(TABLE)
    val url = s"jdbc:clickhouse://$hostName:$port"

    val cluster = Option(options.get(CLUSTER))
    val parsedTable = ClickhouseTableService.fetchFieldsAndEngine(url, table) match {
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read metadata about $table on $url", exception)
      case Success(value) => value
    }

    val clickhouseFields = parsedTable.fields
    val clickhouseTimeZoneInfo = JDBCTimeZoneUtils.fetchClickhouseTimeZoneFromServer(url)

    val schema = StructType(clickhouseFields.map(clickhouseField => clickhouseField.typ.toSparkType match {
      case ArrayType(elementType, _) => StructField(clickhouseField.name, ArrayType(elementType, true))
      case _ => StructField(clickhouseField.name, clickhouseField.typ.toSparkType, /*clickhouse.typ.nullable*/true)
    })
    )

    ClickhouseReaderConfiguration(
      schema = schema,
      tableInfo = TableInfo(
        name = table,
        engine = parsedTable.engine,
        cluster = cluster,
        orderingKey = None
      ),
      url = url,
      rowMapper = rs => clickhouseFields.map(_.extractFromRs(rs)(clickhouseTimeZoneInfo)),
      connectionProperties = MapUtils.toProperties(options)
    )
  }
}
