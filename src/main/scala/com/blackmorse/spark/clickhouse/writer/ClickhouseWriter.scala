package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import scala.util.Using

class ClickhouseWriter(chWriterConf: ClickhouseWriterConfiguration, clickhouseTable: ClickhouseTable, url: String) extends DataWriter[InternalRow] with Logging {

  //TODO unify URLs
  private val jdbcUrl = if(url.startsWith("jdbc")) url else s"jdbc:clickhouse://$url"

  private val connection = new ClickHouseDriver().connect(jdbcUrl, chWriterConf.connectionProps)
  private val fields = chWriterConf.schema.map(_.name).mkString("(", ", ", ")")
  private val values = Array.fill(chWriterConf.schema.size)("?").mkString("(", ", ", ")")
  private val insertStatement = s"INSERT INTO $clickhouseTable $fields VALUES $values"
  private var statement = connection.prepareStatement(insertStatement)
  private var rowsInBatch = 0

  override def write(record: InternalRow): Unit = {
    rowsInBatch += 1
    chWriterConf.rowSetters.foreach(_(record, statement))
    statement.addBatch()
    if (rowsInBatch >= chWriterConf.batchSize) {
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
