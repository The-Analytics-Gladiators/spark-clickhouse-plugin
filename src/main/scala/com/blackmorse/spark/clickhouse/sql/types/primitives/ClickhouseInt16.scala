package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{ShortRSExtractor, SimpleArrayRSExtractor, StandardInternalRowConverter, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, ShortType}

import java.sql.PreparedStatement

case class ClickhouseInt16(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with ShortRSExtractor
    with SimpleArrayRSExtractor
    with StandardInternalRowConverter[Short]
    with StandardRowArrayConverter[Short] {
  override type T = Short
  override lazy val defaultValue: Short = 0

  override def toSparkType: DataType = ShortType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int16

  override def setValueToStatement(i: Int, value: Short, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setShort(i, value)
}