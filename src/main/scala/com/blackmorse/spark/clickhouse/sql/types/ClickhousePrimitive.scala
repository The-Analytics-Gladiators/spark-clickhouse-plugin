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
  def toPrimitiveConstructor(clickHouseDataType: ClickHouseDataType): (Boolean, Boolean) => ClickhouseType = (nullable, lowCardinality) => {
    clickHouseDataType match {
      case ClickHouseDataType.Date => ClickhouseDate(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Date32 => ClickhouseDate32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int8 => ClickhouseInt8(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int16 => ClickhouseInt16(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int32 => ClickhouseInt32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int64 => ClickhouseInt64(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int128 => ClickhouseInt128(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Int256 => ClickhouseInt256(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt8 => ClickhouseUInt8(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt16 => ClickhouseUInt16(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt32 => ClickhouseUInt32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt64 => ClickhouseUInt64(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt128 => ClickhouseUInt128(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.UInt256 => ClickhouseUInt256(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.String => ClickhouseString(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Float32 => ClickhouseFloat32(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Float64 => ClickhouseFloat64(nullable = nullable, lowCardinality = lowCardinality)
      case ClickHouseDataType.Bool => ClickhouseBoolean(nullable = nullable, lowCardinality = lowCardinality)
    }
  }
}