package com.blackmorse.spark.clickhouse.reader

import com.clickhouse.jdbc.ClickHouseConnection
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}

import java.sql.ResultSet

class ClickhouseReaderBase[Partition <: InputPartition](chReaderConf: ClickhouseReaderConfiguration,
                                                        sql: String,
                                                        connectionProvider: () => ClickHouseConnection)
    extends PartitionReader[InternalRow]
    with Logging {

  private val conn = connectionProvider()
  private val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  stmt.setFetchSize(300)
  logDebug(s"statement fetch size set to: ${stmt.getFetchSize}")

  private val rs = stmt.executeQuery()

  override def next(): Boolean = rs.next()

  override def get(): InternalRow = {
    val internalRow = new GenericInternalRow(chReaderConf.schema.size)
    chReaderConf.rowMapper(rs)
      .zip(chReaderConf.schema.fields)
      .zipWithIndex.foreach { case ((v, field), index) =>
      if (v != null) {
        val writer = InternalRow.getWriter(index, field.dataType)
        writer(internalRow, v)
      } else {
        internalRow.setNullAt(index)
      }
    }
    internalRow
  }

  override def close(): Unit = {
    try {
      if (null != rs) {
        rs.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing ResultSet", e)
    }
    try {
      if (null != stmt) {
        stmt.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    try {
      if (null != conn) {
        conn.close()
      }
      logDebug("closed connection")
    } catch {
      case e: Exception => logWarning("Exception closing connection", e)
    }
  }
}
