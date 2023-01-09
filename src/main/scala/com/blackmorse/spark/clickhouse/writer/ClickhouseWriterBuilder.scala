package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.spark.types.SchemaMerger
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.tables.services.TableInfoService
import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.{BATCH_SIZE, CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, CLUSTER, TABLE}
import org.apache.commons.collections.MapUtils
import org.apache.spark.sql.connector.write._

import scala.util.{Failure, Success}

class ClickhouseWriterBuilder(info: LogicalWriteInfo) extends WriteBuilder {
  override def build(): Write = new ClickhouseWrite(info)
}

class ClickhouseWrite(info: LogicalWriteInfo) extends Write {
  override def toBatch: BatchWrite = {
    val (chWriterInfo, clickhouseTable) = writerInfo()
    new ClickhouseBatchWrite(chWriterInfo, clickhouseTable)
  }

  private def writerInfo(): (ClickhouseWriterConfiguration, ClickhouseTable) = {
    val hostName = info.options.get(CLICKHOUSE_HOST_NAME)
    val port = info.options.get(CLICKHOUSE_PORT)
    val table = info.options.get(TABLE)
    val batchSize = Option(info.options.get(BATCH_SIZE))
      .map(_.toInt)
      .getOrElse(1000000
      )
    val cluster = Option(info.options.get(CLUSTER))

    val url = s"jdbc:clickhouse://$hostName:$port"

    val allAvailableProperties = MapUtils.toProperties(info.options().asCaseSensitiveMap())

    (for {
      clickhouseFields <- TableInfoService.fetchFields(url, table, allAvailableProperties)
      clickhouseTimeZoneInfo <- JDBCTimeZoneUtils.fetchClickhouseTimeZoneFromServer(url)
      clickhouseTable <- TableInfoService.readTableInfo(url, table, allAvailableProperties)
    } yield {
      val schema = info.schema()
      val mergedSchema = SchemaMerger.mergeSchemas(schema, clickhouseFields)

      val fields = mergedSchema.zipWithIndex.map { case ((sparkField, chField), index) =>
        Field(chType = chField.typ,
          sparkDataType = sparkField.dataType,
          index = index,
          clickhouseTimeZoneInfo = clickhouseTimeZoneInfo
        )
      }

      (ClickhouseWriterConfiguration(
        url = url,
        batchSize = batchSize,
        cluster = cluster,
        fields = fields,
        schema = schema,
        connectionProps = allAvailableProperties
      ), clickhouseTable)
    }) match {
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read metadata about $table", exception)
      case Success(value) => value
    }
  }
}
