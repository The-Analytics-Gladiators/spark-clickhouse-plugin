package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, DecimalType, IntegerType, LongType, ShortType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import java.math.BigInteger
import java.sql.{PreparedStatement, ResultSet}

abstract class ClickhouseBigIntType(private val _clickHouseDataType: ClickHouseDataType) extends ClickhousePrimitive {
  override type T = String
  override val defaultValue: String = "0"

  override def clickhouseDataType: ClickHouseDataType = _clickHouseDataType
  override def toSparkType(): DataType = StringType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    UTF8String.fromString(resultSet.getString(name))

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef =
    resultSet.getArray(name)
      .getArray.asInstanceOf[Array[BigInteger]]
      .map(bi => if(bi == null) null else UTF8String.fromString(bi.toString()))

  override protected def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, new java.math.BigDecimal(value))

  override def convertInternalValue(value: Any): String =
    value.asInstanceOf[UTF8String].toString
}

case class ClickhouseInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.Int128)
case class ClickhouseUInt128(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.UInt128)
case class ClickhouseInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.Int256)
case class ClickhouseUInt256(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseBigIntType(ClickHouseDataType.UInt256)