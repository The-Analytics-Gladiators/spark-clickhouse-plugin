package com.blackmorse.spark.clickhouse.writer

import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import java.util.Properties

class ClickhouseWriter(writerInfo: ClickhouseWriterInfo) extends DataWriter[InternalRow] with Logging {

  private val connection = new ClickHouseDriver().connect(writerInfo.url, new Properties())
  private val fields = writerInfo.schema.map(_.name).mkString("(", ", ", ")")
  private val values = Array.fill(writerInfo.schema.size)("?").mkString("(", ", ", ")")
  private val insertStatement = s"INSERT INTO ${writerInfo.tableName} $fields VALUES $values"
  private var statement = connection.prepareStatement(insertStatement)
  private var rowsInBatch = 0

  override def write(record: InternalRow): Unit = {
    rowsInBatch += 1
    writerInfo.rowSetters.foreach(rowSetter => rowSetter(record, statement))
    statement.addBatch()
    if (rowsInBatch >= writerInfo.batchSize) {
      statement.execute()
      statement.close()
      statement = connection.prepareStatement(insertStatement)
      rowsInBatch = 0
    }
  }

  override def commit(): WriterCommitMessage = {
    statement.executeBatch()
    new WriterCommitMessage {}
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    try {
      if (null != statement) {
        statement.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      if (null != connection) {
        connection.close()
      }
      logDebug("closed connection")
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
  }
}
