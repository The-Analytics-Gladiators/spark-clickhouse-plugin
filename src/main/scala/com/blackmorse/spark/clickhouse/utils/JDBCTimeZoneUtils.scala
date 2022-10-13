package com.blackmorse.spark.clickhouse.utils

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.jdbc.ClickHouseDriver

import java.time.LocalDate
import java.util.Properties
import scala.util.Using

object JDBCTimeZoneUtils {
  def fetchClickhouseTimeZoneFromServer(url: String): ClickhouseTimeZoneInfo = {
    Using(new ClickHouseDriver().connect(url, new Properties())) { connection =>
      Using(connection.createStatement()) {stmt =>
        val rs = stmt.executeQuery("SELECT timeZone()")
        rs.next()
        val clickhouseTimeZone = rs.getString(1)
        ClickhouseTimeZoneInfo(clickhouseTimeZone)
      }
    }.flatten.get
  }

  def localDateToDate(localDate: LocalDate, clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): java.sql.Date = {
    val instant = localDate.atStartOfDay(clickhouseTimeZoneInfo.calendar.getTimeZone.toZoneId).toInstant
    val utilDate = java.util.Date.from(instant)
    new java.sql.Date(utilDate.getTime)
  }
}
