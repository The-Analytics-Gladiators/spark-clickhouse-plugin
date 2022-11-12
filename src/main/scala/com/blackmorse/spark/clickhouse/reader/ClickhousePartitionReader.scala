package com.blackmorse.spark.clickhouse.reader

import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.sql.ResultSet
import java.util.Properties

class ClickhousePartitionReaderFactory(clickhouseReaderInfo: ClickhouseReaderInfo) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new ClickhousePartitionReader(
      clickhouseReaderInfo = clickhouseReaderInfo,
      clickhouseInputPartition = partition.asInstanceOf[ClickhouseInputPartition])
}

class ClickhousePartitionReader(clickhouseReaderInfo: ClickhouseReaderInfo, clickhouseInputPartition: ClickhouseInputPartition) extends PartitionReader[InternalRow] with Logging {
  private val fields = clickhouseReaderInfo.schema.fields.map(f => s"`${f.name}`").mkString(", ")
  val sql = s"SELECT $fields FROM ${clickhouseReaderInfo.tableName}"

  private val conn = new ClickHouseDriver().connect(clickhouseReaderInfo.url, new Properties())
  private val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  stmt.setFetchSize(300)
  logDebug(s"statement fetch size set to: ${stmt.getFetchSize}")

  private val rs = stmt.executeQuery()

  override def next(): Boolean = rs.next()

  override def get(): InternalRow = {
    val internalRow = new GenericInternalRow(clickhouseReaderInfo.schema.size)
    clickhouseReaderInfo.rowMapper(rs)
      .zip(clickhouseReaderInfo.schema.fields)
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
