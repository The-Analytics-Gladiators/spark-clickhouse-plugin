package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql.{Date, PreparedStatement, ResultSet}
import java.time.LocalDate
import java.util.TimeZone

case class ClickhouseDate32(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = java.sql.Date
  override val defaultValue: Date = new Date(0 - TimeZone.getDefault.getRawOffset)
  override def toSparkType(): DataType = DateType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getDate(name)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date32

  override protected def extractFromRow(i: Int, row: Row): Date = row.getDate(i)

  override protected def setValueToStatement(i: Int, value: Date, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i, value, clickhouseTimeZoneInfo.calendar)

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)
                                       (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name).getArray.asInstanceOf[Array[LocalDate]]
      .map(localDate => localDate.atStartOfDay(clickhouseTimeZoneInfo.calendar.getTimeZone.toZoneId).toInstant)
      .map(instant => java.util.Date.from(instant))
      .map(date => new java.sql.Date(date.getTime))
}
