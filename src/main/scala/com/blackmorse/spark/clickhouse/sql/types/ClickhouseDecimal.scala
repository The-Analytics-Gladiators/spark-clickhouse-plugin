package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{PreparedStatement, ResultSet}

sealed trait DecimalTrait extends ClickhouseType

/**
 * Responsible for Decimal(P, S) Clickhouse type
 */
case class ClickhouseDecimal(p: Int, s: Int, nullable: Boolean) extends DecimalTrait {
  override type T = String

  override lazy val defaultValue: String = "0"

  override def toSparkType(): DataType = StringType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    UTF8String.fromString(resultSet.getString(name))

  protected override def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, new java.math.BigDecimal(value).setScale(s))

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray().asInstanceOf[Array[java.math.BigDecimal]]
      .map(bd => if (bd == null) null else UTF8String.fromString(bd.toString))

  override def clickhouseDataTypeString: String = s"Decimal($p, $s)"

  override def convertInternalValue(value: Any): String =
    value.asInstanceOf[UTF8String].toString
}