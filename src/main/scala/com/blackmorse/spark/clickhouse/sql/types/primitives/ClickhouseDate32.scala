package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DateArrayRSExtractor, DateRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql.{Date, PreparedStatement, ResultSet}
import java.time.LocalDate
import java.util.TimeZone

case class ClickhouseDate32(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DateRSExtractor
    with DateArrayRSExtractor {
  override type T = java.sql.Date
  override val defaultValue: Date = new Date(0 - TimeZone.getDefault.getRawOffset)
  override def toSparkType(): DataType = DateType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date32

  override protected def setValueToStatement(i: Int, value: Date, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i, value, clickhouseTimeZoneInfo.calendar)

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)
                                       (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name).getArray.asInstanceOf[Array[LocalDate]]
      .map(ld => if (ld == null) null else ld.toEpochDay.toInt)

  override def convertInternalValue(value: Any): Date = new Date(value.asInstanceOf[Integer].toLong * 1000 * 60 * 60 * 24)
}
