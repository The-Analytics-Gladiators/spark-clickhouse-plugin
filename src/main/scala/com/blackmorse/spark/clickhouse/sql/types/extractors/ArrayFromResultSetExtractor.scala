package com.blackmorse.spark.clickhouse.sql.types.extractors

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import java.math.BigInteger
import java.sql.{ResultSet, Timestamp}
import java.time.{LocalDate, LocalDateTime}

trait ArrayFromResultSetExtractor {
  def extractArrayFromRsByName(name: String, resultSet: ResultSet)
                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef
}

trait SimpleArrayRSExtractor extends ArrayFromResultSetExtractor {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name).getArray
}

trait StringArrayRSExtractor extends ArrayFromResultSetExtractor {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray
      .asInstanceOf[Array[String]]
      .map(UTF8String.fromString)
}

trait BigIntArrayRSExtractor extends ArrayFromResultSetExtractor {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray
      .asInstanceOf[Array[BigInteger]]
      .map(bi => if (bi == null) null else UTF8String.fromString(bi.toString()))
}

trait DateArrayRSExtractor extends ArrayFromResultSetExtractor {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray
      .asInstanceOf[Array[LocalDate]]
      .map(ld => if (ld == null) null else ld.toEpochDay.toInt)
}

trait UInt64ArrayRSExtractor extends ArrayFromResultSetExtractor { self: ClickhousePrimitive =>
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef = {
    val array = resultSet.getArray(name).getArray
    //Array(Nullable(primitive)) produces Long[], while Array(primitive) -> long[]
    if (this.nullable) {
      val mapper = (l: java.lang.Long) => if (l == null) null else Decimal(l)
      array.asInstanceOf[Array[java.lang.Long]].map(mapper)
    } else {
      val mapper = (l: Long) => Decimal(l)
      array.asInstanceOf[Array[Long]].map(mapper)
    }
  }
}

trait TimestampArrayRSExtractor extends ArrayFromResultSetExtractor {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[LocalDateTime]]
      .map(ldt => if (ldt == null) null else Timestamp.valueOf(ldt).getTime * 1000)
}

trait BigDecimalArrayRSExtractor extends ArrayFromResultSetExtractor {
  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray().asInstanceOf[Array[java.math.BigDecimal]]
      .map(bd => if (bd == null) null else UTF8String.fromString(bd.toString))
}