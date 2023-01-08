  package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{ArrayFromResultSetExtractor, InternalRowValueConverter, TypeFromResultSetExtractor}
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

  def toSparkType: DataType

  def setValueToStatement(i: Int, value: T, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit

  def convertInternalValue(value: Any): T

  def convertInternalArrayValue(value: ArrayData, sparkType: DataType): Seq[T]

  def clickhouseDataTypeString: String
}

case class ClickhouseField(name: String, typ: ClickhouseType) {
  def extractFromRs(resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    typ.extractFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)
  }
}
