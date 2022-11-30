package com.blackmorse.spark.clickhouse.spark.types

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
import com.blackmorse.spark.clickhouse.utils.JDBCUtils
import com.clickhouse.jdbc.ClickHouseDriver

import java.sql.ResultSet
import java.util.Properties
import scala.collection.mutable
import scala.util.{Failure, Success, Try, Using}

case class ClickhouseParsedTable(fields: Seq[ClickhouseField], engine: String)

object ClickhouseSchemaParser {
  private def parseFields(url: String, table: String, connectionProperties: Properties): Try[Seq[ClickhouseField]] =
    JDBCUtils.executeSql(url, connectionProperties)(s"DESCRIBE TABLE $table"){ rs =>
      ClickhouseField(rs.getString(1), ClickhouseTypesParser.parseType(rs.getString(2)))}

  private def parseEngine(url: String, table: String, connectionProperties: Properties): Try[String] =
    JDBCUtils.executeSql(url, connectionProperties)(s"SELECT engine FROM system.tables WHERE database || '.' || name = '$table'")(rs => rs.getString(1))
      .flatMap(seq => Try(seq.head))

  def parseTable(url: String, table: String, connectionProperties: Properties = new Properties()): Try[ClickhouseParsedTable] =
    for {
      fields <- parseFields(url, table, connectionProperties)
      engine <- parseEngine(url, table, connectionProperties)
    } yield ClickhouseParsedTable(fields = fields, engine = engine)
}
