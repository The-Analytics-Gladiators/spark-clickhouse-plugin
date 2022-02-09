package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import java.sql.{PreparedStatement, ResultSet}

trait ClickhouseType {
  val nullable: Boolean
  def toSparkType(): DataType
  def extractFromRs(name: String, resultSet: ResultSet): Any
  def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                     (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit
  def arrayClickhouseTypeString(): String
}

case class ClickhouseField(name: String, typ: ClickhouseType) {
  def extractFromRs(resultSet: ResultSet): Any = {
    typ.extractFromRs(name, resultSet)
  }
}
