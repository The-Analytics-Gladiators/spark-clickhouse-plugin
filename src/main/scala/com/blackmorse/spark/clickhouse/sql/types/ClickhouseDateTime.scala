package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, TimestampType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseDateTime(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = TimestampType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getTimestamp(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setTimestamp(i + 1, row.getTimestamp(i), clickhouseTimeZoneInfo.calendar)

  override def arrayClickhouseTypeString(): String = s"Array(DateTime)"
}

case class ClickhouseDateTime64(p: Int, nullable: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = TimestampType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getTimestamp(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setTimestamp(i + 1, row.getTimestamp(i))

  override def arrayClickhouseTypeString(): String = s"Array(DateTime64($p))"
}
