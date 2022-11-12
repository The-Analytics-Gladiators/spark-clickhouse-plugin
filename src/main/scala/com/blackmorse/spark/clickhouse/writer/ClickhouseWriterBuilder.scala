package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.spark.types.{ClickhouseSchemaParser, SchemaMerger}
import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.{BATCH_SIZE, CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE}
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.sql.PreparedStatement
import java.util.Properties

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

    val clickhouseFields = ClickhouseSchemaParser.parseTable(url, table)
    val schema = info.schema()
    val mergedSchema = SchemaMerger.mergeSchemas(schema, clickhouseFields)

    val rowSetters = mergedSchema.zipWithIndex.map { case ((sparkField, chField), index) =>
      val clickhouseType = chField.typ

      val rowExtractor = (row: InternalRow, index: Int) => InternalRow.getAccessor(sparkField.dataType, clickhouseType.nullable)(row, index)

      (row: InternalRow, statement: PreparedStatement) =>
        clickhouseType.extractFromRowAndSetToStatement(index, row, rowExtractor, statement)(clickhouseTimeZoneInfo)
    }

    ClickhouseWriterInfo(
      url = url,
      tableName = table,
      batchSize = batchSize,
      rowSetters = rowSetters,
      schema = schema
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
