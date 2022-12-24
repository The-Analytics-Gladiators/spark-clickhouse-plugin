package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.DIRECTLY_USE_DISTRIBUTED_TABLE
import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.tables.{ClickhouseTable, DistributedTable}
import com.blackmorse.spark.clickhouse.tables.services.{ClickhouseHost, ClusterService}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}

import scala.util.{Failure, Success}

class ClickhouseBatchWrite(chWriterConf: ClickhouseWriterConfiguration, clickhouseTable: ClickhouseTable) extends BatchWrite {
  private val directlyUseDistributed = Option(chWriterConf.connectionProps.get(DIRECTLY_USE_DISTRIBUTED_TABLE))
    .map(_.asInstanceOf[String].toBoolean)
    .getOrElse(false)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    (chWriterConf.cluster, clickhouseTable) match {
      case (Some(userCluster), DistributedTable(_, _, _, cluster, _)) if userCluster != cluster => throw new IllegalArgumentException(s"User has pointed to the Distributed table: " +
        s"$clickhouseTable which is defined on the $cluster, while user has specified $userCluster")
      case (_, t: DistributedTable) if directlyUseDistributed => new SimpleClickhouseWriterFactory(chWriterConf, t, Seq(ClickhouseHost(1, chWriterConf.url)))
      case (_, DistributedTable(_, _, _, cluster, underlyingTable)) => new SimpleClickhouseWriterFactory(chWriterConf, underlyingTable, getShardUrls(chWriterConf.copy(cluster = Some(cluster))))
      case (Some(_), table) => new SimpleClickhouseWriterFactory(chWriterConf, table, getShardUrls(chWriterConf))
      case (None, table) => new SimpleClickhouseWriterFactory(chWriterConf, table, Seq(ClickhouseHost(1, chWriterConf.url)))
    }
  }

  private def getShardUrls(chWriterConf: ClickhouseWriterConfiguration): Seq[ClickhouseHost] = {
    ClusterService.getShardUrls(chWriterConf.url, chWriterConf.cluster.get, chWriterConf.connectionProps) match {
      case Success(value) => value
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read shards of the cluster ${chWriterConf.cluster} " +
        s"at ${chWriterConf.url}.", exception)
    }
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
