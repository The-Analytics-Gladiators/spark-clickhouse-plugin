package com.blackmorse.spark.clickhouse.reader.sharded

import com.blackmorse.spark.clickhouse.BATCH_SIZE
import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.tables.services.{ClickhouseHost, TableInfoService}
import com.blackmorse.spark.clickhouse.tables.{ClickhouseTable, MergeTreeTable}
import org.apache.spark.sql.connector.read.InputPartition

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

trait PartitionsPlanner {
  def planPartitions(hosts: Seq[ClickhouseHost], table: ClickhouseTable, chReaderConf: ClickhouseReaderConfiguration): Array[InputPartition]
}

trait PerShardPartitionsPlanner extends PartitionsPlanner {
  override def planPartitions(shardsUrls: Seq[ClickhouseHost], table: ClickhouseTable, chReaderConf: ClickhouseReaderConfiguration): Array[InputPartition] =
    shardsUrls.map(host => ShardedClickhousePartition(host.url, None)).toArray
}

trait MergeTreePartitionsPlanner extends PartitionsPlanner {
  private def divWithCeil(a: Long, b: Int): Long =
    if (a % b == 0) a / b else (a / b + 1)
  override def planPartitions(shardsUrls: Seq[ClickhouseHost], table: ClickhouseTable, chReaderConf: ClickhouseReaderConfiguration): Array[InputPartition] = {
    val future = Future.sequence(shardsUrls.map(host => Future(getPartitionsForHost(host, table, chReaderConf))))
      .map(_.flatten)

    Await.result(future, 30.second).toArray
  }

  private def getPartitionsForHost(host: ClickhouseHost, table: ClickhouseTable, chReaderConf: ClickhouseReaderConfiguration): Seq[ShardedClickhousePartition] = {
    val url = host.url
    val count = TableInfoService.getCountRows(s"jdbc:clickhouse://${host.url}", table.toString(), chReaderConf.connectionProps)

    val batchSize = Option(chReaderConf.connectionProps.get(BATCH_SIZE))
      .map(_.asInstanceOf[String].toInt)
      .getOrElse(1000000)

    (0.toLong until divWithCeil(count, batchSize)) map (offset =>
      ShardedClickhousePartition(
        partitionUrl = url,
        limitBy = Some(LimitBy(
          offset = offset * batchSize,
          batchSize = batchSize,
          orderingKey = table.asInstanceOf[MergeTreeTable].orderingKey
        ))))
  }
}
