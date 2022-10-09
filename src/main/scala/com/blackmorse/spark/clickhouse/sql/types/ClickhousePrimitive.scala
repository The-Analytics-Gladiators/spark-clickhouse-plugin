package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.primitives._
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import java.sql.{PreparedStatement, ResultSet}

trait ClickhousePrimitive extends ClickhouseType {
  val nullable: Boolean
  val lowCardinality: Boolean

  def toSparkType(): DataType
  protected def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any

  def clickhouseDataType: ClickHouseDataType

  override def clickhouseDataTypeString: String = clickhouseDataType.toString
}

object ClickhousePrimitive {
  def toPrimitiveConstructor(clickHouseDataType: ClickHouseDataType): (Boolean, Boolean) => ClickhouseType = {
    clickHouseDataType match {
      case ClickHouseDataType.Date => (nullable, lowCardinality) => ClickhouseDate(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Date32 => (nullable, lowCardinality) => ClickhouseDate32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int8 => (nullable, lowCardinality) => ClickhouseInt8(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int16 => (nullable, lowCardinality) => ClickhouseInt16(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int32 => (nullable, lowCardinality) => ClickhouseInt32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int64 => (nullable, lowCardinality) => ClickhouseInt64(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int128 => (nullable, lowCardinality) => ClickhouseInt128(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int256 => (nullable, lowCardinality) => ClickhouseInt256(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt8 => (nullable, lowCardinality) => ClickhouseUInt8(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt16 => (nullable, lowCardinality) => ClickhouseUInt16(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt32 => (nullable, lowCardinality) => ClickhouseUInt32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt64 => (nullable, lowCardinality) => ClickhouseUInt64(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt128 => (nullable, lowCardinality) => ClickhouseUInt128(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt256 => (nullable, lowCardinality) => ClickhouseUInt256(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.String => (nullable, lowCardinality) => ClickhouseString(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Float32 => (nullable, lowCardinality) => ClickhouseFloat32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Float64 => (nullable, lowCardinality) => ClickhouseFloat64(nullable = nullable, lowCardinality = lowCardinality)
    }
  }
}