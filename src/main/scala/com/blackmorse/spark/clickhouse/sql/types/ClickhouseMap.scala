package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseMap(key: ClickhouseType, value: ClickhouseType, nullable: Boolean) extends ClickhouseType {
  override type T = Map[key.T, value.T]

  override def toSparkType(): DataType = ???

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = ???

  override val defaultValue: Map[key.T, value.T] = Map()

  override protected def extractFromRow(i: Int, row: Row): Map[key.T, value.T] = ???

  override protected def setValueToStatement(i: Int, v: Map[key.T, value.T], statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = ???

  override def clickhouseDataTypeString: String = ???
}
