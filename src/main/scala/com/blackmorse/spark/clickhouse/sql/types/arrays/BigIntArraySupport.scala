package com.blackmorse.spark.clickhouse.sql.types.arrays

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.math.BigInteger
import java.sql.ResultSet

trait BigIntArraySupport extends ArraySupport {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[BigInteger]]
      .map(bi => if (bi == null) null else bi.toString)
}
