package com.blackmorse.spark.clickhouse.reader.sharded

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.utils.JDBCUtils
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success}

case class ShardedClickhousePartition(partitionUrl: String, limitBy: Option[LimitBy]) extends InputPartition

case class LimitBy(offset: Long, batchSize: Int, orderingKey: String)

class ClickhouseShardedPartitionScan(val chReaderConf: ClickhouseReaderConfiguration)
    extends Scan
    with Batch {

  //Figuring out http ports of remote clickhouse hosts is not so trivial:
  private val sql =
    s"""
       |SELECT
       |    host_name || ':' || getServerPort('http_port')::String AS http_port
       |FROM clusterAllReplicas(spark_clickhouse_cluster, system, clusters)
       |WHERE is_local = 1
       |LIMIT 1 BY shard_num
       |""".stripMargin

  private val shardsUrls = JDBCUtils.executeSql(chReaderConf.url)(sql){ rs => rs.getString(1)} match {
    case Success(value) => value
    case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read shards of the cluster ${chReaderConf.tableInfo.cluster} " +
          s"at ${chReaderConf.url}. Request: $sql", exception)
  }

  override def readSchema(): StructType = chReaderConf.schema

  override def planInputPartitions(): Array[InputPartition] = shardsUrls.map(url => ShardedClickhousePartition(url, None)).toArray

  override def createReaderFactory(): PartitionReaderFactory = new ClickhouseShardedPartitionReader(chReaderConf)

  override def toBatch: Batch = this
}
