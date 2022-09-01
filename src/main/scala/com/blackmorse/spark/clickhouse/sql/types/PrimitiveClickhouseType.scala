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

  def clickhouseDataType: ClickHouseDataType

  override def clickhouseDataTypeString: String = clickhouseDataType.toString
}

object ClickhousePrimitive {
  def toPrimitiveConstructor(clickHouseDataType: ClickHouseDataType): (Boolean, Boolean) => ClickhouseType = {
    clickHouseDataType match {
      case ClickHouseDataType.Date => ClickhouseDate.apply
      case ClickHouseDataType.Date32 => ClickhouseDate32.apply
      case ClickHouseDataType.Int8 => ClickhouseInt8.apply
      case ClickHouseDataType.Int16 => ClickhouseInt16.apply
      case ClickHouseDataType.Int32 => ClickhouseInt32.apply
      case ClickHouseDataType.Int64 => ClickhouseInt64.apply
      case ClickHouseDataType.Int128 => ClickhouseInt128.apply
      case ClickHouseDataType.Int256 => ClickhouseInt256.apply
      case ClickHouseDataType.UInt8 => ClickhouseUInt8.apply
      case ClickHouseDataType.UInt16 => ClickhouseUInt16.apply
      case ClickHouseDataType.UInt32 => ClickhouseUInt32.apply
      case ClickHouseDataType.UInt64 => ClickhouseUInt64.apply
      case ClickHouseDataType.UInt128 => ClickhouseUInt128.apply
      case ClickHouseDataType.UInt256 => ClickhouseUInt256.apply
      case ClickHouseDataType.String => ClickhouseString.apply
      case ClickHouseDataType.Float32 => ClickhouseFloat32.apply
      case ClickHouseDataType.Float64 => ClickhouseFloat64.apply
    }
  }
}