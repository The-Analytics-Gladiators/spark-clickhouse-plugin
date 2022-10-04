package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, IntegerType, LongType, ShortType, StringType}

import java.math.BigInteger
import java.sql.{PreparedStatement, ResultSet}

abstract class ClickhouseBigIntType(private val _clickHouseDataType: ClickHouseDataType) extends ClickhousePrimitive {
  override type T = String
  override val defaultValue: String = "0"

  override def clickhouseDataType: ClickHouseDataType = _clickHouseDataType
  override def toSparkType(): DataType = StringType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getString(name)

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name).getArray.asInstanceOf[Array[BigInteger]].map(bi => bi.toString())

  override protected def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, new java.math.BigDecimal(value))
}

case class ClickhouseInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.Int128)
case class ClickhouseUInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.UInt128)
case class ClickhouseInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.Int256)
case class ClickhouseUInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.UInt256)

object ClickhouseBigIntType {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => String = (row, index) => sparkType match {
    case ByteType      => row.getByte(index).toString
    case ShortType     => row.getShort(index).toString
    case IntegerType   => row.getInt(index).toString
    case LongType      => row.getLong(index).toString
    case DecimalType() => row.getDecimal(index).toString
    case StringType    => row.getString(index)
  }
}