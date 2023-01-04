package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DoubleRSExtractor, SimpleArrayRSExtractor, StandardInternalRowConverter, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, DoubleType}

import java.sql.PreparedStatement

case class ClickhouseFloat64(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DoubleRSExtractor
    with SimpleArrayRSExtractor
    with StandardInternalRowConverter[Double]
    with StandardRowArrayConverter[Double] {
  override type T = Double
  override val defaultValue: Double = 0.0d

  override def toSparkType: DataType = DoubleType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Float64

  override def setValueToStatement(i: Int, value: Double, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDouble(i, value)
}