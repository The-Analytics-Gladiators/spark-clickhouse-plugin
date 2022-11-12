package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{BigIntArrayRSExtractor, StringRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.PreparedStatement

abstract class ClickhouseBigIntType(private val _clickHouseDataType: ClickHouseDataType)
    extends ClickhousePrimitive
    with StringRSExtractor
    with BigIntArrayRSExtractor {
  override type T = String
  override val defaultValue: String = "0"

  override def clickhouseDataType: ClickHouseDataType = _clickHouseDataType
  override def toSparkType(): DataType = StringType

  override protected def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, new java.math.BigDecimal(value))

  override def convertInternalValue(value: Any): String =
    value.asInstanceOf[UTF8String].toString
}

case class ClickhouseInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.Int128)
case class ClickhouseUInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.UInt128)
case class ClickhouseInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.Int256)
case class ClickhouseUInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.UInt256)