package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}

import scala.util.Random

class ShardedClickhouseWriter(chWriterConf: ClickhouseWriterConfiguration, clickhouseTable: ClickhouseTable, shardUrls: Seq[String])
    extends DataWriter[InternalRow] with Logging {

  private val clickhouseDriver = new ClickHouseDriver()
  private val fields = chWriterConf.schema.map(_.name).mkString("(", ", ", ")")
  private val values = Array.fill(chWriterConf.schema.size)("?").mkString("(", ", ", ")")
  private val insertStatement = s"INSERT INTO $clickhouseTable $fields VALUES $values"

  private val r = new Random()

  private val batchers = shardUrls.map(url =>
    new ClickhouseBatchWriter(clickHouseDriver = clickhouseDriver,
      url = url,
      connectionProps = chWriterConf.connectionProps,
      sql = insertStatement,
      batchSize = chWriterConf.batchSize))

  override def write(record: InternalRow): Unit = {
    val shard = r.nextInt(batchers.size)
    batchers(shard).addRow(record, chWriterConf.fields)
  }

  override def commit(): WriterCommitMessage = {
    batchers.foreach(_.flush())
    new WriterCommitMessage {}
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    batchers.foreach(_.close())
  }
}
