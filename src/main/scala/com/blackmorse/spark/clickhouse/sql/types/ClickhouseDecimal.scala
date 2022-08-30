package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DecimalType}

import java.sql.{PreparedStatement, ResultSet}

sealed trait DecimalTrait extends ClickhouseType

/**
 * Responsible for Decimal(P, S) Clickhouse type
 */
case class ClickhouseDecimal(p: Int, s: Int, nullable: Boolean) extends DecimalTrait {
  override type T = java.math.BigDecimal

  override lazy val defaultValue: java.math.BigDecimal = java.math.BigDecimal.ZERO

  override def toSparkType(): DataType = DecimalType(p, s)

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getBigDecimal(name)

  override def extractFromRow(i: Int, row: Row): java.math.BigDecimal =
    row.getDecimal(i)

  protected override def setValueToStatement(i: Int, value: java.math.BigDecimal, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, value.setScale(s))

  override def clickhouseDataTypeString: String = s"Decimal($p, $s)"
}