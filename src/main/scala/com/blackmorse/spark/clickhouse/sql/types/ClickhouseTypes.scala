package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{ArrayFromResultSetExtractor, TypeFromResultSetExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{PreparedStatement, ResultSet}

trait ClickhouseType
    extends TypeFromResultSetExtractor
    with ArrayFromResultSetExtractor
    with Serializable {
  type T
  val nullable: Boolean

  val defaultValue: T

  def toSparkType(): DataType


  protected def setValueToStatement(i: Int, value: T, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit


  def extractFromRowAndSetToStatement(i: Int, row: InternalRow, rowExtractor: (InternalRow, Int) => Any, statement: PreparedStatement)
                                     (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    val rowIsNull = row.isNullAt(i)
    val statementIndex = i + 1

    if (rowIsNull && nullable) statement.setObject(statementIndex, null)
    else if (rowIsNull) setValueToStatement(statementIndex, defaultValue, statement)(clickhouseTimeZoneInfo)
    else {
      val extracted = rowExtractor(row, i)
      setValueToStatement(statementIndex, convertInternalValue(extracted), statement)(clickhouseTimeZoneInfo)
    }
  }

  def convertInternalValue(value: Any): T = value.asInstanceOf[T]

  def convertInternalArrayValue(value: ArrayData): Seq[T] = value.toSeq(toSparkType())

  def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef = {
    val array = resultSet.getArray(name).getArray
    //TODO
    array match {
      case a: Array[String] => a.map(el => UTF8String.fromString(el))
      case _ => array
    }
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
