package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.utils.{ClickhouseTimeZoneInfo, JDBCTimeZoneUtils}
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql
import java.sql.{Date, PreparedStatement, ResultSet}
import java.time.LocalDate
import java.util.TimeZone

case class ClickhouseDate(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override lazy val defaultValue: sql.Date = new sql.Date(0 - TimeZone.getDefault.getRawOffset)

  type T = java.sql.Date
  override def toSparkType(): DataType = DateType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    val d = resultSet.getDate(name)
    val date = d.toLocalDate
    val millis = JDBCTimeZoneUtils.localDateToDate(date, clickhouseTimeZoneInfo)
      .getTime

    (millis / 1000 / 60 / 60 / 24).toInt
  }


  override protected def setValueToStatement(i: Int, value: sql.Date, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i, value, clickhouseTimeZoneInfo.calendar)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)
                                       (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef = {
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[LocalDate]]
      .map(ld => if (ld == null) null else ld.toEpochDay.toInt)
  }

  override def convertInternalValue(value: Any): sql.Date = new sql.Date(value.asInstanceOf[Integer].toLong * 1000 * 60 * 60 * 24)
}