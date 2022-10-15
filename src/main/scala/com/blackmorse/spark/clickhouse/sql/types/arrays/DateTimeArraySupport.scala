package com.blackmorse.spark.clickhouse.sql.types.arrays

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.sql.{ResultSet, Timestamp}
import java.time.LocalDateTime

trait DateTimeArraySupport extends ArraySupport {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[LocalDateTime]]
      .map(ldt => if (ldt == null) null else Timestamp.valueOf(ldt))
}
