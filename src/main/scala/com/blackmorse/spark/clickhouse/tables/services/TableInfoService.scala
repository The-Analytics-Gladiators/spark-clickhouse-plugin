package com.blackmorse.spark.clickhouse.tables.services

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException
import com.blackmorse.spark.clickhouse.spark.types.ClickhouseTypesParser
import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
import com.blackmorse.spark.clickhouse.tables.{ClickhouseTable, DistributedTable, GenericTable, MergeTreeTable}
import com.blackmorse.spark.clickhouse.utils.JDBCUtils

import java.util.Properties
import scala.util.{Failure, Success, Try}

object TableInfoService {
  def fetchFields(url: String, table: String, connectionProperties: Properties): Try[Seq[ClickhouseField]] =
    JDBCUtils.executeSql(url, connectionProperties)(s"DESCRIBE TABLE $table") { rs =>
      ClickhouseField(rs.getString(1), ClickhouseTypesParser.parseType(rs.getString(2)))
    }

  def readTableInfo(url: String, table: String, connectionProps: Properties): Try[ClickhouseTable] = {
    readNameDatabaseEngine(url, table, connectionProps).flatMap { case (database, name, engine) =>
      engine match {
        case "Distributed" => parseUnderlyingDistributedTable(url, database, name, engine, connectionProps)
        case eng if eng.endsWith("MergeTree") => parseOrderingKeyForMergeTree(url, table, connectionProps)
        case _ => Try(GenericTable(database = database, name = name, engine = engine))
      }
    }
  }

  private def parseUnderlyingDistributedTable(url: String, database: String, table: String, engine: String, connectionProps: Properties): Try[DistributedTable] = {
    for {
      (cluster, underlyingDatabase, underlyingTable) <- readUnderlyingClusterDatabaseAndTable(url, s"$database.$table", connectionProps)
      clickhouseTable <- readTableInfo(url, s"$underlyingDatabase.$underlyingTable", connectionProps)
    } yield DistributedTable(
      database = database,
      name = table,
      engine = engine,
      cluster = cluster,
      underlyingTable = clickhouseTable
    )
  }

  private def readUnderlyingClusterDatabaseAndTable(url: String, tableName: String, connectionProps: Properties): Try[(String, String, String)] = {
    val sql = s"SELECT engine_full FROM system.tables WHERE database || '.' || name = '$tableName'"
    JDBCUtils.executeSql(url, connectionProps)(sql)(rs => rs.getString(1))
      .map(_.head)
      .map(engineFull => TableParserService.parseDistributedUnderlyingTable(engineFull))
  }

  private def readNameDatabaseEngine(url: String, table: String, connectionProps: Properties): Try[(String, String, String)] = {
    val sql = s"SELECT database, name, engine FROM system.tables WHERE database || '.' || name = '$table'"
    JDBCUtils.executeSql(url, connectionProps)(sql)(rs => (rs.getString(1), rs.getString(2), rs.getString(3)))
      .flatMap(seq => Try(seq.head))
  }

  private def parseOrderingKeyForMergeTree(url: String, table: String, connectionProps: Properties): Try[MergeTreeTable] = {
    val sql = s"SELECT database, name, engine, engine_full FROM system.tables WHERE database || '.' || name = '$table'"
    JDBCUtils.executeSql(url, connectionProps)(sql)(rs => (rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)))
      .flatMap(s => Try{s.head})
      .map{ case (database, name, engine, engineFull) => (database, name, engine, TableParserService.parseOrderingKey(engineFull))}
      .map{ case (database, name, engine, orderingKey) =>
        MergeTreeTable(
          database = database,
          name = name,
          engine = engine,
          orderingKey = orderingKey
        )
      }
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
