package com.blackmorse.spark.clickhouse.spark.types

import com.blackmorse.spark.clickhouse.sql.types._
import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseBoolean, ClickhouseDate, ClickhouseDate32, ClickhouseFloat32, ClickhouseFloat64, ClickhouseInt128, ClickhouseInt16, ClickhouseInt256, ClickhouseInt32, ClickhouseInt64, ClickhouseInt8, ClickhouseString, ClickhouseUInt128, ClickhouseUInt16, ClickhouseUInt256, ClickhouseUInt32, ClickhouseUInt64, ClickhouseUInt8}
import com.clickhouse.client.ClickHouseDataType

object ClickhouseTypesParser {
  private val arrayPrefix = "Array("
  private val lowCardinalityPrefix = "LowCardinality("
  private val nullablePrefix = "Nullable("
  private val decimalPrefix = "Decimal("
  private val dateTime = "DateTime"
  private val dateTime64 = "DateTime64("
  private val fixedStringPrefix = "FixedString("

  def parseType(typ: String, lowCardinality: Boolean = false, nullable: Boolean = false): ClickhouseType = {
    val startsWith: (String => Boolean) = prefix => typ.startsWith(prefix)
    if (startsWith(arrayPrefix)) {
      ClickhouseArray(parseType(typ.substring(arrayPrefix.length, typ.length - 1)))
    } else if (startsWith(lowCardinalityPrefix)) {
      parseType(typ.substring(lowCardinalityPrefix.length, typ.length - 1), lowCardinality = true, nullable = nullable)
    } else if (startsWith(nullablePrefix)) {
      parseType(typ.substring(nullablePrefix.length, typ.length - 1), lowCardinality = lowCardinality, nullable = true)
    } else if (startsWith(dateTime64)) {
      val p = typ.substring(dateTime64.length, typ.length - 1).trim.toInt
      ClickhouseDateTime64(p, nullable = nullable)
    } else if (startsWith(dateTime)) {
      ClickhouseDateTime(nullable = nullable, lowCardinality = lowCardinality)
    } else if (startsWith(decimalPrefix)) {
      val rest = typ.substring(decimalPrefix.length, typ.length - 1)
      val split = rest.split(",")
      ClickhouseDecimal(split.head.trim.toInt, split(1).trim.toInt, nullable = nullable)
    } else if (startsWith(fixedStringPrefix)) {
      val length = typ.substring(fixedStringPrefix.length, typ.length - 1)
      ClickhouseFixedString(nullable = nullable, lowCardinality = lowCardinality, length = length.toInt)
    } else {
      toPrimitiveConstructor(ClickHouseDataType.valueOf(typ))(nullable, lowCardinality)
    }
  }
  private def toPrimitiveConstructor(clickHouseDataType: ClickHouseDataType): (Boolean, Boolean) => ClickhouseType = (nullable, lowCardinality) => {
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
