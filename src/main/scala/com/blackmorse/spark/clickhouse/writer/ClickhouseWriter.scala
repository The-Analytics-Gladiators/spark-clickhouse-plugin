package com.blackmorse.spark.clickhouse.writer

import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import scala.util.Using

class ClickhouseWriter(writerInfo: ClickhouseWriterInfo) extends DataWriter[InternalRow] with Logging {

  private val connection = new ClickHouseDriver().connect(writerInfo.url, writerInfo.connectionProperties)
  private val fields = writerInfo.schema.map(_.name).mkString("(", ", ", ")")
  private val values = Array.fill(writerInfo.schema.size)("?").mkString("(", ", ", ")")
  private val insertStatement = s"INSERT INTO ${writerInfo.tableName} $fields VALUES $values"
  private var statement = connection.prepareStatement(insertStatement)
  private var rowsInBatch = 0

  override def write(record: InternalRow): Unit = {
    rowsInBatch += 1
    writerInfo.rowSetters.foreach(_(record, statement))
    statement.addBatch()
    if (rowsInBatch >= writerInfo.batchSize) {
      log.debug(s"Batch complete, sending $rowsInBatch records")
      Using(statement) { stm => stm.executeBatch() }
        .recoverWith { case ex: Exception => throw new Exception(ex) } // consider custom exceptions
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
      case exception: Exception => logWarning("Exception closing statement", exception)
    }
    try {
      if (null != connection) {
        connection.close()
      }
      logDebug("closed connection")
    } catch {
      case exception: Exception => logWarning("Exception closing connection", exception)
    }
  }
}
