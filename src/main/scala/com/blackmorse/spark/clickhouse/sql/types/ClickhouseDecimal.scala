package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DecimalType, StringType}

import java.sql.{PreparedStatement, ResultSet}

sealed trait DecimalTrait extends ClickhouseType

/**
 * Responsible for Decimal(P, S) Clickhouse type
 */
case class ClickhouseDecimal(p: Int, s: Int, nullable: Boolean) extends DecimalTrait {
  override type T = String

  override lazy val defaultValue: String = "0"

  override def toSparkType(): DataType = StringType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getString(name)

  protected override def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, new java.math.BigDecimal(value).setScale(s))

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name).getArray().asInstanceOf[Array[java.math.BigDecimal]].map(_.toString)

  override def clickhouseDataTypeString: String = s"Decimal($p, $s)"
}

object ClickhouseDecimal {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Any = sparkType match {
    case StringType => (row, index) => row.getString(index)
  }
}