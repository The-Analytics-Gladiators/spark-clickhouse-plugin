package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, TimestampType}

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.LocalDateTime

case class ClickhouseDateTime(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = TimestampType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getTimestamp(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setTimestamp(i + 1, row.getTimestamp(i), clickhouseTimeZoneInfo.calendar)

  override def arrayClickhouseTypeString(): String = s"Array(DateTime)"

  override def extractArray(name: String, resultSet: ResultSet): AnyRef =
    resultSet.getArray(name).getArray.asInstanceOf[Array[LocalDateTime]].map(ldt => Timestamp.valueOf(ldt))

  override def extractArrayFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                          (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    //Set timezone to array
    val array = row.getList(i).toArray.map(el => ((el.asInstanceOf[Timestamp].getTime + clickhouseTimeZoneInfo.timeZoneMillisDiff) / 1000).asInstanceOf[Object])
    val jdbcArray = statement.getConnection.createArrayOf(arrayClickhouseTypeString(), array)
    statement.setArray(i + 1, jdbcArray)
  }
}

case class ClickhouseDateTime64(p: Int, nullable: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = TimestampType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getTimestamp(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setTimestamp(i + 1, row.getTimestamp(i))

  override def arrayClickhouseTypeString(): String = s"Array(DateTime64($p))"

  override def extractArray(name: String, resultSet: ResultSet): AnyRef =
    resultSet.getArray(name).getArray.asInstanceOf[Array[LocalDateTime]].map(ldt => Timestamp.valueOf(ldt))
}
