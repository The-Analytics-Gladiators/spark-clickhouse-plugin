package com.blackmorse.spark.clickhouse.sql.types.arrays

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo

import java.sql.ResultSet

trait UnsignedLongArraySupport extends ArraySupport {
  self: ClickhousePrimitive =>

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef = {
    val array = resultSet.getArray(name).getArray
    //Array(Nullable(primitive)) produces Long[], while Array(primitive) -> long[]
    if (nullable) {
      val mapper = (l: java.lang.Long) => if (l == null) null else new java.math.BigDecimal(l.toString)
      array.asInstanceOf[Array[java.lang.Long]].map(mapper)
    } else {
      val mapper = (l: Long) => new java.math.BigDecimal(l.toString)
      array.asInstanceOf[Array[Long]].map(mapper)
    }
  }
}
