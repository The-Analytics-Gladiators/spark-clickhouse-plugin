package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType}

import java.math.BigInteger
import java.sql.{PreparedStatement, ResultSet}

abstract class ClickhouseBigIntType extends ClickhousePrimitive {
  override type T = String
  override val defaultValue: String = "0"

  override def toSparkType(): DataType = StringType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getString(name)

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name).getArray.asInstanceOf[Array[BigInteger]].map(bi => bi.toString())

  override protected def extractFromRow(i: Int, row: Row): String = row.getString(i)

  override protected def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, new java.math.BigDecimal(value))
}

case class ClickhouseInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType {
  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int128
}

case class ClickhouseUInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType {
  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt128
}

case class ClickhouseInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType {
  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int256
}

case class ClickhouseUInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType {
  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt256
}
