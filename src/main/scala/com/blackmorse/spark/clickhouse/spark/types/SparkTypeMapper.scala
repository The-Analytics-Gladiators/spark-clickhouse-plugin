package com.blackmorse.spark.clickhouse.spark.types

import com.blackmorse.spark.clickhouse.sql.types.{ClickhouseArray, ClickhouseDateTime, ClickhouseDecimal, ClickhouseType, PrimitiveClickhouseType}
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

object SparkTypeMapper {
  def mapType(dataType: DataType): ClickhouseType =
    dataType match {
      case BooleanType => PrimitiveClickhouseType(ClickHouseDataType.UInt8, nullable = false, lowCardinality = false)
      case ByteType => PrimitiveClickhouseType(ClickHouseDataType.Int8, nullable = false, lowCardinality = false)
      case IntegerType => PrimitiveClickhouseType(ClickHouseDataType.Int32, nullable = false, lowCardinality = false)
      case LongType => PrimitiveClickhouseType(ClickHouseDataType.Int64, nullable = false, lowCardinality = false)
      case ShortType => PrimitiveClickhouseType(ClickHouseDataType.Int16, nullable = false, lowCardinality = false)
      case FloatType => PrimitiveClickhouseType(ClickHouseDataType.Float32, nullable = false, lowCardinality = false)
      case DoubleType => PrimitiveClickhouseType(ClickHouseDataType.Float64, nullable = false, lowCardinality = false)
      case StringType => PrimitiveClickhouseType(ClickHouseDataType.String, nullable = false, lowCardinality = false)
      case DecimalType() => ClickhouseDecimal(38, 18, nullable = false)
      case DateType => PrimitiveClickhouseType(ClickHouseDataType.Date, nullable = false, lowCardinality = false)
      case TimestampType => ClickhouseDateTime(nullable = false, lowCardinality = false)
      case ArrayType(elementType, _containsNull) => ClickhouseArray(mapType(elementType))
    }
}
