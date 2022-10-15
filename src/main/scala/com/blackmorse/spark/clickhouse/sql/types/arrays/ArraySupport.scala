package com.blackmorse.spark.clickhouse.sql.types.arrays

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.sql.ResultSet

trait ArraySupport {
  def extractArrayFromRsByName(name: String, resultSet: ResultSet)
                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef
}
