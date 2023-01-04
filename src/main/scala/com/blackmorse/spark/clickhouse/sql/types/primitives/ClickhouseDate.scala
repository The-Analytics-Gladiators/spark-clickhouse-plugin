package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DateArrayRSExtractor, DateIntervalRowConverter, DateRSExtractor, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql
import java.sql.PreparedStatement
import java.util.TimeZone

case class ClickhouseDate(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DateRSExtractor
    with DateArrayRSExtractor
    with DateIntervalRowConverter
    with StandardRowArrayConverter[java.sql.Date] {
  override lazy val defaultValue: sql.Date = new sql.Date(0 - TimeZone.getDefault.getRawOffset)

  type T = java.sql.Date
  override def toSparkType: DataType = DateType

  override def setValueToStatement(i: Int, value: sql.Date, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i, value, clickhouseTimeZoneInfo.calendar)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date
}