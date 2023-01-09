package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.tables.services.ClickhouseHost
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

class ShardedClickhouseWriterFactory(chWriterConf: ClickhouseWriterConfiguration,
                                     clickhouseTable: ClickhouseTable,
                                     clickhouseHosts: Seq[ClickhouseHost]) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new ShardedClickhouseWriter(
      chWriterConf = chWriterConf,
      clickhouseTable = clickhouseTable,
      shardUrls = clickhouseHosts.map(_.url)
    )
  }
}
