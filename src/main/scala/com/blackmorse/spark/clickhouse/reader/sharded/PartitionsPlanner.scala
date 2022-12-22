package com.blackmorse.spark.clickhouse.reader.sharded

import com.blackmorse.spark.clickhouse.BATCH_SIZE
import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.services.ClickhouseTableService
import com.blackmorse.spark.clickhouse.tables.services.TableInfoService
import com.blackmorse.spark.clickhouse.tables.{ClickhouseTable, MergeTreeTable}
import org.apache.spark.sql.connector.read.InputPartition

trait PartitionsPlanner {
  def planPartitions(shardsUrls: Seq[String], table: ClickhouseTable, chReaderConf: ClickhouseReaderConfiguration): Array[InputPartition]
}

trait PerShardPartitionsPlanner extends PartitionsPlanner {
  override def planPartitions(shardsUrls: Seq[String], table: ClickhouseTable, chReaderConf: ClickhouseReaderConfiguration): Array[InputPartition] =
    shardsUrls.map(url => ShardedClickhousePartition(url, None)).toArray
}

trait MergeTreePartitionsPlanner extends PartitionsPlanner {
  private def divWithCeil(a: Long, b: Int): Long =
    if (a % b == 0) a / b else (a / b + 1)
  override def planPartitions(shardsUrls: Seq[String], table: ClickhouseTable, chReaderConf: ClickhouseReaderConfiguration): Array[InputPartition] = {
    val batchSize = Option(chReaderConf.connectionProps.get(BATCH_SIZE))
      .map(_.asInstanceOf[String].toInt)
      .getOrElse(1000000)


    shardsUrls.par.map(url => (url, TableInfoService.getCountRows(s"jdbc:clickhouse://$url", table.toString(), chReaderConf.connectionProps)))
      .flatMap {
        case (url, count) =>
          (0.toLong until divWithCeil(count, batchSize)) map (offset =>
            ShardedClickhousePartition(
              partitionUrl = url,
              limitBy = Some(LimitBy(
                offset = offset * batchSize,
                batchSize = batchSize,
                orderingKey = table.asInstanceOf[MergeTreeTable].orderingKey
              ))))
      }.toArray
  }
}