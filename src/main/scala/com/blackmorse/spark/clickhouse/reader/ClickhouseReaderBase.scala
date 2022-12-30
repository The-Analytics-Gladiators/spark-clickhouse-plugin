package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.USE_FORCE_COLLAPSING_MODIFIER
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.clickhouse.jdbc.ClickHouseConnection
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}

import java.sql.ResultSet

class ClickhouseReaderBase[Partition <: InputPartition](chReaderConf: ClickhouseReaderConfiguration,
                                                        table: ClickhouseTable,
                                                        sql: String,
                                                        connectionProvider: () => ClickHouseConnection)
    extends PartitionReader[InternalRow]
    with Logging {

  private val useForceCollapsingModifier = Option(chReaderConf.connectionProps.get(USE_FORCE_COLLAPSING_MODIFIER))
    .exists(_.asInstanceOf[String].toBoolean)

  private val sqlQuery: String = table.engine match {
    case engine if useForceCollapsingModifier &&
      (engine.contains("CollapsingMergeTree") || engine.contains("ReplacingMergeTree")) => s"$sql FINAL"
    case _ => sql
  }

  private val conn = connectionProvider()
  private val stmt = conn.prepareStatement(sqlQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
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
