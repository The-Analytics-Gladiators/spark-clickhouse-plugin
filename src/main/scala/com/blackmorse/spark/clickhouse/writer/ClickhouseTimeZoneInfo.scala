package com.blackmorse.spark.clickhouse.writer

import java.util.{Calendar, TimeZone}

case class ClickhouseTimeZoneInfo(calendar: Calendar, timeZoneMillisDiff: Int)

object ClickhouseTimeZoneInfo {
  def apply(clickhouseTimeZone: String): ClickhouseTimeZoneInfo = {
    val cal = Calendar.getInstance(TimeZone.getTimeZone(clickhouseTimeZone))
    val timeZoneMillisDiff = TimeZone.getDefault.getRawOffset - cal.getTimeZone.getRawOffset
    ClickhouseTimeZoneInfo(cal, timeZoneMillisDiff)
  }
}


