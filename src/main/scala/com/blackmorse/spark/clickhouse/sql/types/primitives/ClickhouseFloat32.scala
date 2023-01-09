package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{FloatRSEXtractor, SimpleArrayRSExtractor, StandardInternalRowConverter, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, FloatType}

import java.sql.PreparedStatement

case class ClickhouseFloat32(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with FloatRSEXtractor
    with SimpleArrayRSExtractor
    with StandardInternalRowConverter[Float]
    with StandardRowArrayConverter[Float] {
  override type T = Float
  override val defaultValue: Float = 0.0f

  override def toSparkType: DataType = FloatType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Float32

  override def setValueToStatement(i: Int, value: Float, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setFloat(i, value)
}