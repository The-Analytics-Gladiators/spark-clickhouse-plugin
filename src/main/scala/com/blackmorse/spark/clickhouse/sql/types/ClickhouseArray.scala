package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType}

import java.sql.{PreparedStatement, ResultSet, Timestamp}

case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType {
  override def toSparkType(): DataType = ArrayType(typ.toSparkType(), typ.nullable)
  override val nullable: Boolean = false

  override def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    typ.extractArray(name, resultSet)(clickhouseTimeZoneInfo)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    typ.extractArrayFromRowAndSetToStatement(i, row, statement)(clickhouseTimeZoneInfo)
  }

  override def arrayClickhouseTypeString(): String = s"Array(${typ.arrayClickhouseTypeString()})"
}
