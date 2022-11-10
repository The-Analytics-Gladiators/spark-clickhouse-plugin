package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{TimestampArrayRSExtractor, TimestampRSExtractor}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, TimestampType}

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.LocalDateTime
import java.util.TimeZone

case class ClickhouseDateTime(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhouseType
    with TimestampRSExtractor
    with TimestampArrayRSExtractor {
  override type T = Timestamp
  override lazy val defaultValue = new Timestamp(0 - TimeZone.getDefault.getRawOffset)

  override def toSparkType(): DataType = TimestampType

  override protected def setValueToStatement(i: Int, value: Timestamp, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setTimestamp(i, value, clickhouseTimeZoneInfo.calendar)

  override def clickhouseDataTypeString: String = "DateTime"
}

object ClickhouseDateTime {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Any = sparkType match {
    case TimestampType => (row, index) => row.getTimestamp(index)
  }
}

case class ClickhouseDateTime64(p: Int, nullable: Boolean)
    extends ClickhouseType
    with TimestampRSExtractor
    with TimestampArrayRSExtractor {
  override type T = Timestamp
  override lazy val defaultValue: Timestamp = new Timestamp(0 - TimeZone.getDefault.getRawOffset)
  override def toSparkType(): DataType = TimestampType

  protected override def setValueToStatement(i: Int, value: Timestamp, statement: PreparedStatement)
                                            (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    statement.setTimestamp(i, value, clickhouseTimeZoneInfo.calendar)
  }

  override def clickhouseDataTypeString: String = s"DateTime64($p)"
}

object ClickhouseDateTime64 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Timestamp = (row, index) => sparkType match {
    case TimestampType => row.getTimestamp(index)
  }
}