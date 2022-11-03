package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, TimestampType}

import java.sql.{PreparedStatement, ResultSet, Timestamp}
import java.time.LocalDateTime
import java.util.TimeZone

case class ClickhouseDateTime(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType {
  override type T = Timestamp
  override lazy val defaultValue = new Timestamp(0 - TimeZone.getDefault.getRawOffset)

  override def toSparkType(): DataType = TimestampType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getTimestamp(name).getTime * 1000

  override protected def setValueToStatement(i: Int, value: Timestamp, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setTimestamp(i, value, clickhouseTimeZoneInfo.calendar)

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[LocalDateTime]]
      .map(ldt => if(ldt == null) null else Timestamp.valueOf(ldt).getTime * 1000)

  override def clickhouseDataTypeString: String = "DateTime"
  //  //For some reason timezone is preserved while reading an array
//  override def extractArrayFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
//                                          (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
//    //Set timezone to array
//    val array = row.getList(i).toArray.map(el => ((el.asInstanceOf[Timestamp].getTime + clickhouseTimeZoneInfo.timeZoneMillisDiff) / 1000).asInstanceOf[Object])
//    val jdbcArray = statement.getConnection.createArrayOf(arrayClickhouseTypeString(), array)
//    statement.setArray(i + 1, jdbcArray)
//  }
}

object ClickhouseDateTime {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Any = sparkType match {
    case TimestampType => (row, index) => row.getTimestamp(index)
  }
}

case class ClickhouseDateTime64(p: Int, nullable: Boolean) extends ClickhouseType {
  override type T = Timestamp
  override lazy val defaultValue: Timestamp = new Timestamp(0 - TimeZone.getDefault.getRawOffset)
  override def toSparkType(): DataType = TimestampType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getTimestamp(name).getTime * 1000

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[LocalDateTime]]
      .map(ldt => if (ldt == null) null else Timestamp.valueOf(ldt).getTime * 1000)

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