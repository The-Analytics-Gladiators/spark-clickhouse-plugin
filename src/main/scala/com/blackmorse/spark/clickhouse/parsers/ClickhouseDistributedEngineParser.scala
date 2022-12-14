package com.blackmorse.spark.clickhouse.parsers

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderConfiguration, TableInfo}
import com.blackmorse.spark.clickhouse.reader.sharded.ClickhouseShardedPartitionScan
import com.blackmorse.spark.clickhouse.utils.JDBCUtils

import java.util.Properties
import scala.util.{Failure, Success, Try}

object ClickhouseDistributedEngineParser {
  private val distributedPrefix = "Distributed("
  def parseDistributedUnderlyingTable(engineFull: String): (String, String, String) = {
    val inside = engineFull.substring(distributedPrefix.length, engineFull.length - 1)
    val array = inside.split(",")
      .map(_.trim)
      .map(s => s.substring(1, s.length - 1))

    (array.head, array(1), array(2))
  }

  def getUnderlyingTable(clickhouseReaderInfo: ClickhouseReaderConfiguration): TableInfo = {
    val (cluster, database, table) = getUnderlyingTableParams(clickhouseReaderInfo.url, clickhouseReaderInfo.tableInfo.name, clickhouseReaderInfo.connectionProperties)
    match {
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to identify underlying table for ${clickhouseReaderInfo.tableInfo.name}", exception)
      case Success(value) => value
    }

    clickhouseReaderInfo.tableInfo.cluster
      .foreach(userCluster => if (userCluster != cluster) throw new IllegalArgumentException(s"User has pointed to the Distributed table: " +
        s"${clickhouseReaderInfo.tableInfo.name}, which is defined on the $cluster, while user" +
        s" has specified ${clickhouseReaderInfo.tableInfo.cluster.get}"))

    TableInfo(
      name = s"$database.$table",
      engine = "Doesntmatteryet",
      cluster = Some(cluster)
    )
  }

  private def getUnderlyingTableParams(url: String, table: String, properties: Properties): Try[(String, String, String)] = {
    val sql = s"SELECT engine_full from system.tables where database || '.' || name = '$table'"
    println(sql)
    JDBCUtils.executeSql(url, properties)(sql)(rs => rs.getString(1))
      .map(_.head)
      .map(engineFull => ClickhouseDistributedEngineParser.parseDistributedUnderlyingTable(engineFull))
  }
}
