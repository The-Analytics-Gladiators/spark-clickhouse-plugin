package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{StringArrayRSExtractor, StringRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.PreparedStatement

case class ClickhouseFixedString(nullable: Boolean, lowCardinality: Boolean, length: Int)
    extends ClickhouseType
    with StringRSExtractor
    with StringArrayRSExtractor {
  override type T = String
  override val defaultValue: String = " " * length

  override def toSparkType(): DataType = StringType

  override protected def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    statement.setString(i, value)
  }

  override def clickhouseDataTypeString: String = s"FixedString($length)"

  override def convertInternalValue(value: Any): String =
    value.asInstanceOf[UTF8String].toString
}