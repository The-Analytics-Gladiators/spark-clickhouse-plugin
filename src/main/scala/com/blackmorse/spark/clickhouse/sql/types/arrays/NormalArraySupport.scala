package com.blackmorse.spark.clickhouse.sql.types.arrays

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.sql.ResultSet

trait NormalArraySupport extends ArraySupport {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef
  = resultSet.getArray(name).getArray
}
