package com.blackmorse.spark.clickhouse

import org.threeten.extra.Days

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, ZoneId, ZonedDateTime}

object DateTimeUtils {

  def localDateToDate(localDate: LocalDate): Date =
    new Date(localDate.atStartOfDay(ZoneId.systemDefault())
      .toInstant
      .getEpochSecond * 1000)

  def date(offset: Int): Date =
    localDateToDate(
      LocalDate.now().plus(Days.of(offset))
    )

  def time(offset: Int): Timestamp =
    new Timestamp(
      ZonedDateTime.now(ZoneId.of("UTC"))
        .plusHours(offset)
        .toInstant
        .getEpochSecond * 1000
    )

}
