package com.blackmorse.spark.clickhouse.services

import com.blackmorse.spark.clickhouse.spark.types.ClickhouseTypesParser
import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
import com.blackmorse.spark.clickhouse.utils.JDBCUtils

import java.util.Properties
import scala.util.Try

case class ClickhouseParsedTable(fields: Seq[ClickhouseField], engine: String)

@deprecated("Remove after Writer refactoring")
object ClickhouseTableService {
  def fetchFields(url: String, table: String, connectionProperties: Properties): Try[Seq[ClickhouseField]] =
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
}
