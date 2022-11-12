package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{BigDecimalArrayRSExtractor, StringRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.PreparedStatement

sealed trait DecimalTrait extends ClickhouseType

/**
 * Responsible for Decimal(P, S) Clickhouse type
 */
case class ClickhouseDecimal(p: Int, s: Int, nullable: Boolean)
    extends DecimalTrait
    with StringRSExtractor
    with BigDecimalArrayRSExtractor {
  override type T = String

  override lazy val defaultValue: String = "0"

  override def toSparkType(): DataType = StringType

  protected override def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, new java.math.BigDecimal(value).setScale(s))

  override def clickhouseDataTypeString: String = s"Decimal($p, $s)"

  override def convertInternalValue(value: Any): String =
    value.asInstanceOf[UTF8String].toString
}