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
  override def toSparkType(): DataType = DecimalType(p, s)

  override def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getBigDecimal(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {

//    val value = if(s == 0) {
//      //in case of *Int* values getting rid from fractional part for saving precision
//      row.getDecimal(i).setScale(0)
//    } else {
//      row.getDecimal(i)
//    }
    statement.setBigDecimal(i + 1, row.getDecimal(i).setScale(s))
  }

  override def arrayClickhouseTypeString(): String =
    s"Array(Decimal($p, $s))"
}