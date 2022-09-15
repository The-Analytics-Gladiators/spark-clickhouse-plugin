package com.blackmorse.spark.clickhouse.spark.types

import com.blackmorse.spark.clickhouse.sql.types.primitives._
import com.blackmorse.spark.clickhouse.sql.types._
import org.apache.spark.sql.types._

object SparkTypeMapper {
  def mapType(dataType: DataType, chType: ClickhouseType): ClickhouseType =
    (dataType, chType) match {
      case (BooleanType, _) => ClickhouseUInt8(nullable = false, lowCardinality = false)
      case (ByteType, _) => ClickhouseInt8(nullable = false, lowCardinality = false)
      case (IntegerType, _) => ClickhouseInt32(nullable = false, lowCardinality = false)
      case (LongType, _) => ClickhouseInt64(nullable = false, lowCardinality = false)
      case (ShortType, _) => ClickhouseInt16(nullable = false, lowCardinality = false)
      case (FloatType, _) => ClickhouseFloat32(nullable = false, lowCardinality = false)
      case (DoubleType, _) => ClickhouseFloat64(nullable = false, lowCardinality = false)

      case (StringType, t @ ClickhouseInt128(_, _)) => t
      case (StringType, t @ ClickhouseInt256(_, _)) => t
      case (StringType, t @ ClickhouseUInt128(_, _)) => t
      case (StringType, t @ ClickhouseUInt256(_, _)) => t
      case (StringType, t @ ClickhouseDecimal(_, _, _)) => t

      case (StringType, _) => ClickhouseString(nullable = false, lowCardinality = false)

      case (DecimalType(), ClickhouseDecimal(p, s, nullable)) =>
        ClickhouseDecimal(p, s, nullable = nullable)
      case(DecimalType(), t @ ClickhouseUInt64(_, _)) => t
      case (DecimalType(), t: ClickhousePrimitive) if t.clickhouseDataType.toString.contains("Int") =>
        ClickhouseDecimal(38, 0, t.nullable)
      case (DecimalType(), _) =>
        ClickhouseDecimal(38, 18, nullable = false)

      case (DateType, _) => ClickhouseDate(nullable = false, lowCardinality = false)
      case (TimestampType, ClickhouseDateTime(_, _)) => chType
      case (TimestampType, ClickhouseDateTime64(_, _)) => chType
      case (ArrayType(elementType, _), ClickhouseArray(typ)) =>
        ClickhouseArray(mapType(elementType, typ))
    }
}
