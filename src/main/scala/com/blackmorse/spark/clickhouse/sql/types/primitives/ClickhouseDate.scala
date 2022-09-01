package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql
import java.sql.{PreparedStatement, ResultSet}
import java.time.LocalDate
import java.util.{Date, TimeZone}

trait ClickhouseDateBase extends ClickhousePrimitive {
  val nullable: Boolean
  val lowCardinality: Boolean

  override lazy val defaultValue: sql.Date = new sql.Date(0 - TimeZone.getDefault.getRawOffset)

  type T = java.sql.Date

  override def toSparkType(): DataType = DateType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getDate(name)

  override protected def extractFromRow(i: Int, row: Row): sql.Date = row.getDate(i)

  override protected def setValueToStatement(i: Int, value: sql.Date, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i, value, clickhouseTimeZoneInfo.calendar)

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)
                                       (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name).getArray.asInstanceOf[Array[LocalDate]]
      .map(localDate => localDate.atStartOfDay(clickhouseTimeZoneInfo.calendar.getTimeZone.toZoneId).toInstant)
      .map(instant => Date.from(instant))
      .map(date => new java.sql.Date(date.getTime))
}

case class ClickhouseDate(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseDateBase {
  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date
}

case class ClickhouseDate32(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseDateBase {
  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date32
}
