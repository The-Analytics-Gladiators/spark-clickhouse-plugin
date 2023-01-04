package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{ByteRSExtractor, SimpleArrayRSExtractor, StandardInternalRowConverter, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{ByteType, DataType}

import java.sql.PreparedStatement

case class ClickhouseInt8(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with ByteRSExtractor
    with SimpleArrayRSExtractor
    with StandardInternalRowConverter[Byte]
    with StandardRowArrayConverter[Byte] {
  override type T = Byte
  override lazy val defaultValue: Byte = 0

  override def toSparkType: DataType = ByteType

  override def setValueToStatement(i: Int, value: Byte, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setByte(i, value)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int8
}