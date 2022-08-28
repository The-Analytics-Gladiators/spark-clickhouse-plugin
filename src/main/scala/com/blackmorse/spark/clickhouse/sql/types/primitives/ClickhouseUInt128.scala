package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DecimalType}

import java.math.BigInteger
import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseUInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = java.math.BigDecimal
  override val defaultValue: java.math.BigDecimal = java.math.BigDecimal.ZERO

  override def toSparkType(): DataType = DecimalType(38, 0)

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getBigDecimal(name).setScale(0)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt128

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray
      .asInstanceOf[Array[BigInteger]]
      .map(bi => new java.math.BigDecimal(bi))

  override protected def extractFromRow(i: Int, row: Row): java.math.BigDecimal = row.getDecimal(i)

  override protected def setValueToStatement(i: Int, value: java.math.BigDecimal, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, value)
}
