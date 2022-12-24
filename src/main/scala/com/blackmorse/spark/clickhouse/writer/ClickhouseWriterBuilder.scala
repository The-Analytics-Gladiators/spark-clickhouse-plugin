package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.spark.types.SchemaMerger
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.tables.services.TableInfoService
import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.{BATCH_SIZE, CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, CLUSTER, TABLE}
import org.apache.commons.collections.MapUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._

import java.sql.PreparedStatement
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
    val batchSize = info.options.get(BATCH_SIZE).toInt
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

      val rowSetters = mergedSchema.zipWithIndex.map { case ((sparkField, chField), index) =>
        (row: InternalRow, statement: PreparedStatement) =>
          chField.typ.extractFromRowAndSetToStatement(index, row, sparkField.dataType, statement)(clickhouseTimeZoneInfo)
      }

      (ClickhouseWriterConfiguration(
        url = url,
        batchSize = batchSize,
        cluster = cluster,
        rowSetters = rowSetters,
        schema = schema,
        connectionProps = allAvailableProperties
      ), clickhouseTable)
    }) match {
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read metadata about $table", exception)
      case Success(value) => value
    }
  }
}
