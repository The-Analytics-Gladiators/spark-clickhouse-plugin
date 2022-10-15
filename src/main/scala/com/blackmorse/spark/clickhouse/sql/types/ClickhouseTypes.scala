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
    val rowIsNull = row.isNullAt(i)
    val statementIndex = i + 1

    if (rowIsNull && nullable) statement.setObject(statementIndex, null)
    else if (rowIsNull) setValueToStatement(statementIndex, defaultValue, statement)(clickhouseTimeZoneInfo)
    else setValueToStatement(statementIndex, rowExtractor(row, i).asInstanceOf[T], statement)(clickhouseTimeZoneInfo)
  }

  def clickhouseDataTypeString: String

  def mapFromArray(value: Any): AnyRef = value match {
    case null => if (nullable) null else defaultValue.asInstanceOf[AnyRef]
    case el => el.asInstanceOf[AnyRef]
  }
}

case class ClickhouseField(name: String, typ: ClickhouseType) {
  def extractFromRs(resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    typ.extractFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)
  }
}
