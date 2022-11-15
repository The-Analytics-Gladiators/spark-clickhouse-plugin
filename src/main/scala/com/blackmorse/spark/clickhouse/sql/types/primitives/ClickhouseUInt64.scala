package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DecimalInternalRowConverter, DecimalRSExtractor, UInt64ArrayRSExtractor, UInt64InternalRowConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, DecimalType}

import java.sql.PreparedStatement

case class ClickhouseUInt64(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DecimalRSExtractor
    with UInt64ArrayRSExtractor
    with DecimalInternalRowConverter
    with UInt64InternalRowConverter {
  override type T = java.math.BigDecimal
  override val defaultValue: java.math.BigDecimal = java.math.BigDecimal.ZERO

  override def toSparkType: DataType = DecimalType(38, 0)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt64

  override protected def setValueToStatement(i: Int, value: java.math.BigDecimal, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, value)
}