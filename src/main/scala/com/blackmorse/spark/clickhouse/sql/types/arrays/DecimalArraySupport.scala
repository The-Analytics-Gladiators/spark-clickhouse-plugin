package com.blackmorse.spark.clickhouse.sql.types.arrays

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.sql.ResultSet

trait DecimalArraySupport extends ArraySupport {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray().asInstanceOf[Array[java.math.BigDecimal]]
      .map(bd => if (bd == null) null else bd.toString)
}
