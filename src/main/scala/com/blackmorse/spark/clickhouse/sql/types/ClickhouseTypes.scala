package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import java.sql.{PreparedStatement, ResultSet}

trait ClickhouseType {
  type T
  val nullable: Boolean

  val defaultValue: T

  def toSparkType(): DataType

  def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any

  protected def extractFromRow(i: Int, row: Row): T

  protected def setValueToStatement(i: Int, value: T, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit

  def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    if (row.isNullAt(i)) {
      setValueToStatement(i + 1, defaultValue, statement)(clickhouseTimeZoneInfo)
    } else {
      setValueToStatement(i + 1, extractFromRow(i, row), statement)(clickhouseTimeZoneInfo)
    }
  }

  def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef
    = resultSet.getArray(name).getArray

  def clickhouseDataTypeString: String
}

case class ClickhouseField(name: String, typ: ClickhouseType) {
  def extractFromRs(resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    typ.extractFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)
  }
}
