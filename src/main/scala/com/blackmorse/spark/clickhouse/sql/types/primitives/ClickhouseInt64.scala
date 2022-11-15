package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{LongRSExtractor, SimpleArrayRSExtractor, StandardInternalRowConverter, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, LongType}

import java.sql.PreparedStatement

case class ClickhouseInt64(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with LongRSExtractor
    with SimpleArrayRSExtractor
    with StandardInternalRowConverter[Long]
    with StandardRowArrayConverter[Long] {
  override type T = Long
  override val defaultValue: Long = 0

  override def toSparkType: DataType = LongType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int64

  override protected def setValueToStatement(i: Int, value: Long, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setLong(i, value)
}