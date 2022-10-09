package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import java.sql.{PreparedStatement, ResultSet}

trait ClickhouseType extends Serializable {
  type T
  val nullable: Boolean

  val defaultValue: T

  def toSparkType(): DataType

  protected def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any

  def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    if (resultSet.getObject(name) == null) {
      null
    } else {
      extractNonNullableFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)
    }
  }

  protected def setValueToStatement(i: Int, value: T, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit

  def extractFromRowAndSetToStatement(i: Int, row: Row, rowExtractor: (Row, Int) => Any, statement: PreparedStatement)
                                     (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    if (row.isNullAt(i)) {
      if (nullable) {
        statement.setObject(i + 1, null)
      } else {
        setValueToStatement(i + 1, defaultValue, statement)(clickhouseTimeZoneInfo)
      }
    } else {
      setValueToStatement(i + 1, rowExtractor(row, i).asInstanceOf[T], statement)(clickhouseTimeZoneInfo)
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
