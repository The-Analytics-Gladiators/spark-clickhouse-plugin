package com.blackmorse.spark.clickhouse.sql.types.arrays

import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.sql.ResultSet
import java.time.LocalDate

trait DateArraySupport extends ArraySupport {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)
                                       (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[LocalDate]]
      .map(localDate => if (localDate == null) null else JDBCTimeZoneUtils.localDateToDate(localDate, clickhouseTimeZoneInfo))
}
