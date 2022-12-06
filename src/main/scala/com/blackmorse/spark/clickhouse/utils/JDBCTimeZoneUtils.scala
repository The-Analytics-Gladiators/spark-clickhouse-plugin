package com.blackmorse.spark.clickhouse.utils

import com.blackmorse.spark.clickhouse.exceptions.ClickhouseUnableToReadMetadataException

import java.time.LocalDate
import scala.util.{Failure, Success}

object JDBCTimeZoneUtils {
  def fetchClickhouseTimeZoneFromServer(url: String): ClickhouseTimeZoneInfo =
    JDBCUtils.executeSql(url)("SELECT timeZone()"){rs => ClickhouseTimeZoneInfo(rs.getString(1))}
      .map(_.head) match {
      case Failure(exception) => throw ClickhouseUnableToReadMetadataException("Unable to read timeZone() from Clickhouse", exception)
      case Success(value) => value
    }

  def localDateToDate(localDate: LocalDate, clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): java.sql.Date = {
    val instant = localDate.atStartOfDay(clickhouseTimeZoneInfo.calendar.getTimeZone.toZoneId).toInstant
    val utilDate = java.util.Date.from(instant)
    new java.sql.Date(utilDate.getTime)
  }
}
