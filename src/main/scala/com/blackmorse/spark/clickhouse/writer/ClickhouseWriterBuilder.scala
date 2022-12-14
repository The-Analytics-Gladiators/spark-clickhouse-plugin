package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.parsers.ClickhouseSchemaParser
import com.blackmorse.spark.clickhouse.spark.types.SchemaMerger
import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.{BATCH_SIZE, CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE}
import org.apache.commons.collections.MapUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._

import java.sql.PreparedStatement
import scala.util.{Failure, Success}

class ClickhouseWriterBuilder(info: LogicalWriteInfo) extends WriteBuilder {
  override def build(): Write = new ClickhouseWrite(info)
}

class ClickhouseWrite(info: LogicalWriteInfo) extends Write {
  override def toBatch: BatchWrite = new ClickhouseBatchWrite(writerInfo())

  private def writerInfo(): ClickhouseWriterInfo = {
    val hostName = info.options.get(CLICKHOUSE_HOST_NAME)
    val port = info.options.get(CLICKHOUSE_PORT)
    val table = info.options.get(TABLE)
    val batchSize = info.options.get(BATCH_SIZE).toInt

    val url = s"jdbc:clickhouse://$hostName:$port"
    val clickhouseTimeZoneInfo = JDBCTimeZoneUtils.fetchClickhouseTimeZoneFromServer(url)

    val allAvailableProperties = MapUtils.toProperties(info.options().asCaseSensitiveMap())
    val parsedTable = ClickhouseSchemaParser.parseTable(url, table, allAvailableProperties) match {
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read metadata about $table on $url", exception)
      case Success(value) => value
    }
    val clickhouseFields = parsedTable.fields

    val schema = info.schema()
    val mergedSchema = SchemaMerger.mergeSchemas(schema, clickhouseFields)

    val rowSetters = mergedSchema.zipWithIndex.map { case ((sparkField, chField), index) =>
      val clickhouseType = chField.typ

      (row: InternalRow, statement: PreparedStatement) =>
        clickhouseType.extractFromRowAndSetToStatement(index, row, sparkField.dataType, statement)(clickhouseTimeZoneInfo)
    }

    ClickhouseWriterInfo(
      url = url,
      tableName = table,
      batchSize = batchSize,
      rowSetters = rowSetters,
      schema = schema,
      connectionProperties = allAvailableProperties
    )
  }
}

class ClickhouseBatchWrite(writerInfo: ClickhouseWriterInfo) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    new ClickhouseWriterFactory(writerInfo)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class ClickhouseWriterFactory(writerInfo: ClickhouseWriterInfo) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new ClickhouseWriter(writerInfo)
}
