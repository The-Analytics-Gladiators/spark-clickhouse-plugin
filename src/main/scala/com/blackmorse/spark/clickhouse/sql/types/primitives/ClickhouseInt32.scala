package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{IntRSExtractor, SimpleArrayRSExtractor, StandardInternalRowConverter, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, IntegerType}

import java.sql.PreparedStatement

case class ClickhouseInt32(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with IntRSExtractor
    with SimpleArrayRSExtractor
    with StandardInternalRowConverter[Int]
    with StandardRowArrayConverter[Int] {
  override type T = Int
  override val defaultValue: Int = 0

  override def toSparkType: DataType = IntegerType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int32

  override protected def setValueToStatement(i: Int, value: Int, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setInt(i, value)
}