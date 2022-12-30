package com.blackmorse.spark.clickhouse.reader.sharded

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.tables.ClickhouseTable
import com.blackmorse.spark.clickhouse.tables.services.ClusterService
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success}

case class ShardedClickhousePartition(partitionUrl: String, limitBy: Option[LimitBy]) extends InputPartition

case class LimitBy(offset: Long, batchSize: Int, orderingKey: Option[String])

abstract class ClickhouseShardedPartitionScan(val chReaderConf: ClickhouseReaderConfiguration, val table: ClickhouseTable)
    extends Scan
    with Batch
    with PartitionsPlanner {

  //Sharded read should be on cluster
  private val cluster: String = chReaderConf.cluster.get

  private lazy val shardsUrls = ClusterService.getShardUrls(chReaderConf.url, cluster, chReaderConf.connectionProps) match {
    case Success(value) => value
    case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read shards of the cluster $cluster " +
      s"at ${chReaderConf.url}.", exception)
  }

  override def readSchema(): StructType = chReaderConf.schema
  override def planInputPartitions(): Array[InputPartition] =
    planPartitions(shardsUrls, table, chReaderConf)

  override def createReaderFactory(): PartitionReaderFactory = new ClickhouseShardedPartitionReaderFactory(chReaderConf, table)

  override def toBatch: Batch = this
}
