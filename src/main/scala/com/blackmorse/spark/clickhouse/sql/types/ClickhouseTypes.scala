package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import java.sql.{PreparedStatement, ResultSet}

trait ClickhouseType {
  val nullable: Boolean

  def toSparkType(): DataType

  def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any

  def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                     (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit

  def arrayClickhouseTypeString(): String

  def extractArray(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef
    = resultSet.getArray(name).getArray

  def extractArrayFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                     (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    val array = row.getList(i).toArray
    val jdbcArray = statement.getConnection.createArrayOf(arrayClickhouseTypeString(), array)
    statement.setArray(i + 1, jdbcArray)
  }

}

case class ClickhouseField(name: String, typ: ClickhouseType) {
  def extractFromRs(resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    typ.extractFromRs(name, resultSet)(clickhouseTimeZoneInfo)
  }
}
