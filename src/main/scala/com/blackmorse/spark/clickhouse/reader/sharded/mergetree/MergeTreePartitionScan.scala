package com.blackmorse.spark.clickhouse.reader.sharded.mergetree

import com.blackmorse.spark.clickhouse.BATCH_SIZE
import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.reader.ClickhouseReaderConfiguration
import com.blackmorse.spark.clickhouse.reader.sharded.{ClickhouseShardedPartitionReader, LimitBy, ShardedClickhousePartition}
import com.blackmorse.spark.clickhouse.services.ClickhouseTableService
import com.blackmorse.spark.clickhouse.utils.JDBCUtils
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success}

class MergeTreePartitionScan(val chReaderConf: ClickhouseReaderConfiguration) extends Scan with Batch {
  private val batchSize = Option(chReaderConf.connectionProperties.get(BATCH_SIZE))
    .map(_.asInstanceOf[String].toInt)
    .getOrElse(1000000)

  private val table = chReaderConf.tableInfo.name
  private val cluster: Option[String] = chReaderConf.tableInfo.cluster
  //Figuring out http ports of remote clickhouse hosts is not so trivial:
  private val sql =
    s"""
       |SELECT
       |    host_name || ':' || getServerPort('http_port')::String AS http_port
       |FROM clusterAllReplicas(${cluster.get}, system, clusters)
       |WHERE is_local = 1
       |LIMIT 1 BY shard_num
       |""".stripMargin

  private val shardsUrls = JDBCUtils.executeSql(chReaderConf.url)(sql) { rs => rs.getString(1) } match {
    case Success(value) => value
    case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read shards of the cluster $cluster " +
      s"at ${chReaderConf.url}. Request: $sql", exception)
  }

  override def readSchema(): StructType = chReaderConf.schema

  private def divWithCeil(a: Long, b: Int): Long =
    if (a % b == 0) a / b else (a / b + 1)

  override def planInputPartitions(): Array[InputPartition] = {
    val r = shardsUrls.par.map(url => (url, ClickhouseTableService.getCountRows(s"jdbc:clickhouse://$url", table, chReaderConf.connectionProperties)))
        .flatMap {
          case (url, count) =>
            (0.toLong until divWithCeil(count, batchSize)) map (offset =>
              ShardedClickhousePartition(
                partitionUrl = url,
                limitBy = Some(LimitBy(
                  offset = offset * batchSize,
                  batchSize = batchSize,
                  orderingKey = chReaderConf.tableInfo.orderingKey.get
                ))))
        }.toArray

    r.map(_.limitBy).foreach(println)

    r.map(_.asInstanceOf[InputPartition])
  }

  override def createReaderFactory(): PartitionReaderFactory = new ClickhouseShardedPartitionReader(chReaderConf)

  override def toBatch: Batch = this
}
