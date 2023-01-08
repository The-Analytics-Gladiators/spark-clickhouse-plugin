package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.tables.services.{ClickhouseHost, ClusterService}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

import scala.util.{Failure, Random, Success}

class ClickhouseWriterFactory(chWriterConf: ClickhouseWriterConfiguration,
                              clickhouseTable: ClickhouseTable,
                              clickhouseHosts: Seq[ClickhouseHost]) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = chWriterConf.shardingStrategy match {
    case SparkPartition =>
      // Choosing one specific host for a partition
      val urlIndex = partitionId % clickhouseHosts.size
      val host = clickhouseHosts.sortBy(_.shardNum).apply(urlIndex)

      new GenericShardedClickhouseWriter(
        chWriterConf = chWriterConf,
        clickhouseTable = clickhouseTable,
        shardingFunction = _ => 0,
        shardUrls = Seq(host.url)
      )
    case RandomShuffle =>
      val r = new Random()

      new GenericShardedClickhouseWriter(
        chWriterConf = chWriterConf,
        clickhouseTable = clickhouseTable,
        shardingFunction = _ => r.nextInt(clickhouseHosts.size),
        shardUrls = clickhouseHosts.map(_.url)
      )

    case ShardByField(fieldName) =>
      val field = chWriterConf.fields.find(_.name == fieldName)
        .getOrElse(throw new IllegalArgumentException(s"No $fieldName present at the DataFrame"))

      new GenericShardedClickhouseWriter(
        chWriterConf = chWriterConf,
        clickhouseTable = clickhouseTable,
        shardingFunction = internalRow => {
          //TODO code duplication for extracting the value from internalRow
          val isNull = internalRow.isNullAt(field.index)
          if (isNull && field.chType.nullable) {
           0
         } else if(isNull) {
            Math.abs(field.chType.defaultValue.hashCode() % clickhouseHosts.size)
         } else {
            val extracted = InternalRow.getAccessor(field.sparkDataType, field.chType.nullable)(internalRow, field.index)
            Math.abs(field.chType.convertInternalValue(extracted).hashCode() % clickhouseHosts.size)
         }
        },
        shardUrls = clickhouseHosts.map(_.url)
      )
  }
}
