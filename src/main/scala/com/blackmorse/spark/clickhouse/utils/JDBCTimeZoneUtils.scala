package com.blackmorse.spark.clickhouse.utils

import java.time.LocalDate
import scala.util.Try

object JDBCTimeZoneUtils {
  def fetchClickhouseTimeZoneFromServer(url: String): Try[ClickhouseTimeZoneInfo] =
    JDBCUtils.executeSql(url)("SELECT timeZone()"){rs => ClickhouseTimeZoneInfo(rs.getString(1))}
      .map(_.head)

  def localDateToDate(localDate: LocalDate, clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): java.sql.Date = {
    val instant = localDate.atStartOfDay(clickhouseTimeZoneInfo.calendar.getTimeZone.toZoneId).toInstant
    val utilDate = java.util.Date.from(instant)
    new java.sql.Date(utilDate.getTime)
  }
}
