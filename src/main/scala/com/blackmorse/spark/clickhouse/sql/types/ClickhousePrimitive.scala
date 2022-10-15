package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.arrays.{ArraySupport, BigIntArraySupport, DateArraySupport, NormalArraySupport, UnsignedLongArraySupport}
import com.blackmorse.spark.clickhouse.sql.types.primitives._
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.DataType

import java.sql.ResultSet

trait ClickhousePrimitive extends ClickhouseType {
  val nullable: Boolean
  val lowCardinality: Boolean

  def toSparkType(): DataType
  protected def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any

  def clickhouseDataType: ClickHouseDataType

  override def clickhouseDataTypeString: String = clickhouseDataType.toString
}

object ClickhousePrimitive {
  def toPrimitiveConstructor(clickHouseDataType: ClickHouseDataType): (Boolean, Boolean) => ClickhouseType with ArraySupport = (nullable, lowCardinality) => {
    clickHouseDataType match {
      case ClickHouseDataType.Date => new ClickhouseDate(nullable = nullable, lowCardinality = lowCardinality) with DateArraySupport
      case ClickHouseDataType.Date32 => new ClickhouseDate32(nullable = nullable, lowCardinality = lowCardinality) with DateArraySupport
      case ClickHouseDataType.Int8 => new ClickhouseInt8(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.Int16 => new ClickhouseInt16(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.Int32 => new ClickhouseInt32(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.Int64 => new ClickhouseInt64(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.Int128 => new ClickhouseInt128(nullable = nullable, lowCardinality = lowCardinality) with BigIntArraySupport
      case ClickHouseDataType.Int256 => new ClickhouseInt256(nullable = nullable, lowCardinality = lowCardinality) with BigIntArraySupport
      case ClickHouseDataType.UInt8 => new ClickhouseUInt8(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.UInt16 => new ClickhouseUInt16(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.UInt32 => new ClickhouseUInt32(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.UInt64 => new ClickhouseUInt64(nullable = nullable, lowCardinality = lowCardinality) with UnsignedLongArraySupport
      case ClickHouseDataType.UInt128 => new ClickhouseUInt128(nullable = nullable, lowCardinality = lowCardinality) with BigIntArraySupport
      case ClickHouseDataType.UInt256 => new ClickhouseUInt256(nullable = nullable, lowCardinality = lowCardinality) with BigIntArraySupport
      case ClickHouseDataType.String => new ClickhouseString(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.Float32 => new ClickhouseFloat32(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.Float64 => new ClickhouseFloat64(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
      case ClickHouseDataType.Bool => new ClickhouseBoolean(nullable = nullable, lowCardinality = lowCardinality) with NormalArraySupport
    }
  }
}