package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseMap(key: ClickhouseType, value: ClickhouseType, nullable: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = ???

  override def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = ???

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = ???

  override def arrayClickhouseTypeString(): String = ???
}
