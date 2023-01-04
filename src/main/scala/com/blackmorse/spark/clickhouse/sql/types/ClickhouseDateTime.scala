package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{DateTimeInternalRowConverter, DateTimeRowArrayConverter, TimestampArrayRSExtractor, TimestampRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.types.{DataType, TimestampType}

import java.sql.{PreparedStatement, Timestamp}

case class ClickhouseDateTime(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhouseType
    with TimestampRSExtractor
    with TimestampArrayRSExtractor
    with DateTimeInternalRowConverter
    with DateTimeRowArrayConverter {
  override type T = Timestamp
  override lazy val defaultValue = new Timestamp(0)

  override def toSparkType: DataType = TimestampType

  override def setValueToStatement(i: Int, value: Timestamp, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setTimestamp(i, value, clickhouseTimeZoneInfo.calendar)

  override def clickhouseDataTypeString: String = "DateTime"
}

case class ClickhouseDateTime64(p: Int, nullable: Boolean)
    extends ClickhouseType
    with TimestampRSExtractor
    with TimestampArrayRSExtractor
    with DateTimeInternalRowConverter
    with DateTimeRowArrayConverter {
  override type T = Timestamp
  override lazy val defaultValue: Timestamp = new Timestamp(0)
  override def toSparkType: DataType = TimestampType

  override def setValueToStatement(i: Int, value: Timestamp, statement: PreparedStatement)
                                            (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    statement.setTimestamp(i, value, clickhouseTimeZoneInfo.calendar)
  }

  override def clickhouseDataTypeString: String = s"DateTime64($p)"
}