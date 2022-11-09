package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseFixedString(nullable: Boolean, lowCardinality: Boolean, length: Int) extends ClickhouseType {
  override type T = String
  override val defaultValue: String = " " * length

  override def toSparkType(): DataType = StringType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    UTF8String.fromString(resultSet.getString(name))

  override protected def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    statement.setString(i, value)
  }

  override def clickhouseDataTypeString: String = s"FixedString($length)"

}

object ClickhouseFixedString {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => String = (row, index) => sparkType match {
    case StringType => row.getString(index)
  }
}