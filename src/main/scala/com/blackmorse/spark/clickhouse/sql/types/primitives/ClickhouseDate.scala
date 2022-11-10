package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DateArrayRSExtractor, DateRSExtractor}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql
import java.sql.PreparedStatement
import java.util.{Date, TimeZone}

case class ClickhouseDate(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DateRSExtractor
    with DateArrayRSExtractor {
  override lazy val defaultValue: sql.Date = new sql.Date(0 - TimeZone.getDefault.getRawOffset)

  type T = java.sql.Date
  override def toSparkType(): DataType = DateType

  override protected def setValueToStatement(i: Int, value: sql.Date, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i, value, clickhouseTimeZoneInfo.calendar)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date
}

object ClickhouseDate {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Date = (row, index) => sparkType match {
    case DateType => row.getDate(index)
  }
}