package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.tables.services.TableInfoService
import com.blackmorse.spark.clickhouse.utils.{JDBCTimeZoneUtils, PropertiesUtils}
import org.apache.commons.collections.MapUtils
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.util.{Failure, Success}
//for cross-compilation
import scala.collection.JavaConverters._

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = getReaderInfo(options)._1.schema

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val (chReaderConf, table) = getReaderInfo(properties)
    new DatasourceClickhouseTable(chReaderConf, table)
  }

  private def getReaderInfo(options: util.Map[String, String]): (ClickhouseReaderConfiguration, ClickhouseTable) = {
    val hostName = options.get(CLICKHOUSE_HOST_NAME)
    val port = options.getOrDefault(CLICKHOUSE_PORT, "8123")
    val table = options.get(TABLE)
    val url = s"jdbc:clickhouse://$hostName:$port"

    val connectionProps = MapUtils.toProperties(options)
    connectionProps.put("custom_http_params", PropertiesUtils.httpParams(options.asScala.toMap))

    val cluster = Option(options.get(CLUSTER))
    (for {
      clickhouseFields <- TableInfoService.fetchFields(url, table,connectionProps)
      clickhouseTimeZoneInfo <- JDBCTimeZoneUtils.fetchClickhouseTimeZoneFromServer(url)
      clickhouseTable <- TableInfoService.readTableInfo(url, table, connectionProps)
    } yield {
      val chReaderConf = ClickhouseReaderConfiguration(
        schema = clickhouseFieldsToSparkSchema(clickhouseFields),
        cluster = cluster,
        url = url,
        rowMapper = rs => clickhouseFields.map(_.extractFromRs(rs)(clickhouseTimeZoneInfo)),
        connectionProps = connectionProps)
      (chReaderConf, clickhouseTable)
    })
    match {
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read metadata about $table on $url", exception)
      case Success(value) => value
    }
  }

  private def clickhouseFieldsToSparkSchema(clickhouseFields: Seq[ClickhouseField]) =
    StructType(clickhouseFields.map(clickhouseField => clickhouseField.typ.toSparkType match {
      case ArrayType(elementType, _) => StructField(clickhouseField.name, ArrayType(elementType, true))
      case _ => StructField(clickhouseField.name, clickhouseField.typ.toSparkType, /*clickhouse.typ.nullable*/ true)
    })
    )
}
