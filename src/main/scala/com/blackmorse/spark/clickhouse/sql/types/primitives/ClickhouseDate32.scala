package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DateArrayRSExtractor, DateIntervalRowConverter, DateRSExtractor, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql.{Date, PreparedStatement}
import java.util.TimeZone

case class ClickhouseDate32(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DateRSExtractor
    with DateArrayRSExtractor
    with DateIntervalRowConverter
    with StandardRowArrayConverter[java.sql.Date] {
  override type T = java.sql.Date
  override val defaultValue: Date = new Date(0 - TimeZone.getDefault.getRawOffset)
  override def toSparkType: DataType = DateType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date32

  override protected def setValueToStatement(i: Int, value: Date, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i, value, clickhouseTimeZoneInfo.calendar)
}
