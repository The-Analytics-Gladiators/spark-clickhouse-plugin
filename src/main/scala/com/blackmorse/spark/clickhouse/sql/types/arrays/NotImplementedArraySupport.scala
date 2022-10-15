package com.blackmorse.spark.clickhouse.sql.types.arrays
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.sql.ResultSet

trait NotImplementedArraySupport extends ArraySupport {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    throw new UnsupportedOperationException("Not implemented")
}
