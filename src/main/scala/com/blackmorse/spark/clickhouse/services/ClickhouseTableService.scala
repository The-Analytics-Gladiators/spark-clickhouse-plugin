package com.blackmorse.spark.clickhouse.services

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderConfiguration, TableInfo}
import com.blackmorse.spark.clickhouse.spark.types.ClickhouseTypesParser
import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
import com.blackmorse.spark.clickhouse.utils.JDBCUtils

import java.util.Properties
import scala.util.{Failure, Success, Try}

case class ClickhouseParsedTable(fields: Seq[ClickhouseField], engine: String)

object ClickhouseTableService {
  private def fetchFields(url: String, table: String, connectionProperties: Properties): Try[Seq[ClickhouseField]] =
    JDBCUtils.executeSql(url, connectionProperties)(s"DESCRIBE TABLE $table"){ rs =>
      ClickhouseField(rs.getString(1), ClickhouseTypesParser.parseType(rs.getString(2)))}

  private def fetchEngine(url: String, table: String, connectionProperties: Properties): Try[String] =
    JDBCUtils.executeSql(url, connectionProperties)(s"SELECT engine FROM system.tables WHERE database || '.' || name = '$table'")(rs => rs.getString(1))
      .flatMap(seq => Try(seq.head))

  def fetchFieldsAndEngine(url: String, table: String, connectionProperties: Properties = new Properties()): Try[ClickhouseParsedTable] =
    for {
      fields <- fetchFields(url, table, connectionProperties)
      engine <- fetchEngine(url, table, connectionProperties)
    } yield ClickhouseParsedTable(fields = fields, engine = engine)

  def getUnderlyingTableForDistributed(chReaderConf: ClickhouseReaderConfiguration): TableInfo =
    (for {
      (cluster, database, table) <- getUnderlyingClusterDatabaseAndTable(chReaderConf)
      fullTableName = s"$database.$table"
      _ = checkCluster(chReaderConf.tableInfo.cluster, cluster, fullTableName)
      tableInfo <- getTableInfo(chReaderConf.url, fullTableName, cluster, chReaderConf.connectionProperties)
    } yield tableInfo)
      .get

  private def checkCluster(userClusterOpt: Option[String], chCluster: String, tableName: String): Unit =
    userClusterOpt.foreach(userCluster =>
      if (userCluster != chCluster) throw new IllegalArgumentException(s"User has pointed to the Distributed table: " +
        s"$tableName, which is defined on the $chCluster, while user" +
        s" has specified $userCluster"))

  private def getUnderlyingClusterDatabaseAndTable(chReaderConf: ClickhouseReaderConfiguration): Try[(String, String, String)] = {
    val sql = s"SELECT engine_full FROM system.tables WHERE database || '.' || name = '${chReaderConf.tableInfo.name}'"
    JDBCUtils.executeSql(chReaderConf.url, chReaderConf.connectionProperties)(sql)(rs => rs.getString(1))
      .map(_.head)
      .map(engineFull => ClickhouseDistributedTableService.parseDistributedUnderlyingTable(engineFull))
  }

  def getTableInfo(url: String, table: String, cluster: String, properties: Properties): Try[TableInfo] = {
    val sql = s"SELECT engine, engine_full FROM system.tables where database || '.' || name = '$table'"
    JDBCUtils.executeSql(url, properties)(sql)(rs => (rs.getString(1), rs.getString(2)))
      .map(_.head)
      .map{case (engine, engineFull) => TableInfo(
        name = table,
        engine = engine,
        cluster = Some(cluster),
        orderingKey = ClickhouseDistributedTableService.parseOrderingKey(engineFull)
      )}
  }

  def getCountRows(url: String, table: String, properties: Properties): Long = {
    val sql = s"SELECT count() from $table"
    JDBCUtils.executeSql(url, properties)(sql)(rs => rs.getLong(1))
      .map(_.head) match {
      case Success(value) => value
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException(s"Unable to read count of table $table from $url",
        exception)
    }
  }
}
