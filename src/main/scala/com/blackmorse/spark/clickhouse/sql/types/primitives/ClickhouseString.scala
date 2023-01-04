package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{StandardRowArrayConverter, StringArrayRSExtractor, StringInternalRowConverter, StringRSExtractor, StringRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, StringType}

import java.sql.PreparedStatement

case class ClickhouseString(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with StringRSExtractor
    with StringArrayRSExtractor
    with StringInternalRowConverter
    with StringRowArrayConverter {
  override type T = String
  override val defaultValue: String = ""

  override def toSparkType: DataType = StringType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.String

  override def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setString(i, value)
}