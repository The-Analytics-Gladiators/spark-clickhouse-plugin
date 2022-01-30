package com.blackmorse.spark.clickhouse.sql.types

import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import java.sql.ResultSet

case class PrimitiveClickhouseType(typ: ClickHouseDataType, nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = {
    typ match {
      case ClickHouseDataType.Int8 => ByteType
      case ClickHouseDataType.UInt8 => ShortType
      case ClickHouseDataType.Int16 => ShortType
      case ClickHouseDataType.UInt16 => IntegerType
      case ClickHouseDataType.Int32 => IntegerType
      case ClickHouseDataType.UInt32 => LongType
      case ClickHouseDataType.Int64 => LongType
      case ClickHouseDataType.UInt64 => DecimalType(38, 0)
      case ClickHouseDataType.Int128 => DecimalType(38, 0)
      case ClickHouseDataType.UInt128 => DecimalType(38, 0)
      case ClickHouseDataType.Int256 => DecimalType(38, 0)
      case ClickHouseDataType.UInt256 => DecimalType(38, 0)

      case ClickHouseDataType.String => StringType
      case ClickHouseDataType.Float32 => FloatType
      case ClickHouseDataType.Float64 => DoubleType
      case ClickHouseDataType.Date => DateType
      case ClickHouseDataType.Date32 => DateType
    }
  }

  override def extractFromRs(name: String, resultSet: ResultSet): Any =
    typ match {
      case ClickHouseDataType.Int8 => resultSet.getByte(name)
      case ClickHouseDataType.UInt8 => resultSet.getShort(name)
      case ClickHouseDataType.Int16 => resultSet.getShort(name)
      case ClickHouseDataType.UInt16 => resultSet.getInt(name)
      case ClickHouseDataType.Int32 => resultSet.getInt(name)
      case ClickHouseDataType.UInt32 => resultSet.getLong(name)
      case ClickHouseDataType.Int64 => resultSet.getLong(name)
      case ClickHouseDataType.UInt64 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.Int128 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.UInt128 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.Int256 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.UInt256 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.Date => resultSet.getDate(name)
      case ClickHouseDataType.Date32 => resultSet.getDate(name)

      case ClickHouseDataType.String => resultSet.getString(name)
      case ClickHouseDataType.Float32 => resultSet.getFloat(name)
      case ClickHouseDataType.Float64 => resultSet.getDouble(name)
    }
}
