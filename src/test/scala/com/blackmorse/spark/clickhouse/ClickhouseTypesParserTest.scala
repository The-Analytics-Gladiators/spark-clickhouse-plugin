package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.reader.ClickhouseTypesParser
import com.blackmorse.spark.clickhouse.sql.types._
import com.clickhouse.client.ClickHouseDataType
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.propspec.AnyPropSpec

class ClickhouseTypesParserTest extends AnyPropSpec
  with TableDrivenPropertyChecks
  with should.Matchers {

  val primitives = Table(
    "primitives",
    "Int8" -> PrimitiveClickhouseType(ClickHouseDataType.Int8, false, false),
    "Int16" -> PrimitiveClickhouseType(ClickHouseDataType.Int16, false, false),
    "Int32" -> PrimitiveClickhouseType(ClickHouseDataType.Int32, false, false),
    "Int64" -> PrimitiveClickhouseType(ClickHouseDataType.Int64, false, false),
    "Int128" -> PrimitiveClickhouseType(ClickHouseDataType.Int128, false, false),
    "Int256" -> PrimitiveClickhouseType(ClickHouseDataType.Int256, false, false),
    "UInt8" -> PrimitiveClickhouseType(ClickHouseDataType.UInt8, false, false),
    "UInt16" -> PrimitiveClickhouseType(ClickHouseDataType.UInt16, false, false),
    "UInt32" -> PrimitiveClickhouseType(ClickHouseDataType.UInt32, false, false),
    "UInt64" -> PrimitiveClickhouseType(ClickHouseDataType.UInt64, false, false),
    "UInt128" -> PrimitiveClickhouseType(ClickHouseDataType.UInt128, false, false),
    "UInt256" -> PrimitiveClickhouseType(ClickHouseDataType.UInt256, false, false),
    "String" -> PrimitiveClickhouseType(ClickHouseDataType.String, false, false),
    "Float32" -> PrimitiveClickhouseType(ClickHouseDataType.Float32, false, false),
    "Float64" -> PrimitiveClickhouseType(ClickHouseDataType.Float64, false, false),
    "Date" -> PrimitiveClickhouseType(ClickHouseDataType.Date, false, false),
    "Date32" -> PrimitiveClickhouseType(ClickHouseDataType.Date32, false, false),
    "DateTime" -> ClickhouseDateTime(false, false),
  )

  property("Parsing simple primitive types") {
    forAll(primitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfPrimitives = Table(
    "arrayOfPrimitives",
    "Array(Int8)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int8, false, false)),
    "Array(Int16)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int16, false, false)),
    "Array(Int32)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int32, false, false)),
    "Array(Int64)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int64, false, false)),
    "Array(Int128)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int128, false, false)),
    "Array(Int256)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int256, false, false)),
    "Array(UInt8)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt8, false, false)),
    "Array(UInt16)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt16, false, false)),
    "Array(UInt32)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt32, false, false)),
    "Array(UInt64)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt64, false, false)),
    "Array(UInt128)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt128, false, false)),
    "Array(UInt256)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt256, false, false)),
    "Array(String)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.String, false, false)),
    "Array(Float32)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float32, false, false)),
    "Array(Float64)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float64, false, false)),
    "Array(Date)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date, false, false)),
    "Array(Date32)" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date32, false, false)),
    "Array(DateTime)" -> ClickhouseArray(ClickhouseDateTime(false, false)),
  )

  property("Parsing array of simple primitive types") {
    forAll(arrayOfPrimitives) { case(typ, expectedResult) =>
    ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val lowCardinalityofPrimitives = Table(
    "lowCardinalityOfPrimitives",
    "LowCardinality(Int8)" -> PrimitiveClickhouseType(ClickHouseDataType.Int8, false, true),
    "LowCardinality(Int16)" -> PrimitiveClickhouseType(ClickHouseDataType.Int16, false, true),
    "LowCardinality(Int32)" -> PrimitiveClickhouseType(ClickHouseDataType.Int32, false, true),
    "LowCardinality(Int64)" -> PrimitiveClickhouseType(ClickHouseDataType.Int64, false, true),
    "LowCardinality(Int128)" -> PrimitiveClickhouseType(ClickHouseDataType.Int128, false, true),
    "LowCardinality(Int256)" -> PrimitiveClickhouseType(ClickHouseDataType.Int256, false, true),
    "LowCardinality(UInt8)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt8, false, true),
    "LowCardinality(UInt16)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt16, false, true),
    "LowCardinality(UInt32)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt32, false, true),
    "LowCardinality(UInt64)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt64, false, true),
    "LowCardinality(UInt128)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt128, false, true),
    "LowCardinality(UInt256)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt256, false, true),
    "LowCardinality(String)" -> PrimitiveClickhouseType(ClickHouseDataType.String, false, true),
    "LowCardinality(Float32)" -> PrimitiveClickhouseType(ClickHouseDataType.Float32, false, true),
    "LowCardinality(Float64)" -> PrimitiveClickhouseType(ClickHouseDataType.Float64, false, true),
    "LowCardinality(Date)" -> PrimitiveClickhouseType(ClickHouseDataType.Date, false, true),
    "LowCardinality(Date32)" -> PrimitiveClickhouseType(ClickHouseDataType.Date32, false, true),
    "LowCardinality(DateTime)" -> ClickhouseDateTime(false, true),
  )

  property("Parsing LowCardinality of primitive types") {
    forAll(lowCardinalityofPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val nullableOfPrimitives = Table(
    "nullableOfPrimitives",
    "Nullable(Int8)" -> PrimitiveClickhouseType(ClickHouseDataType.Int8, true, false),
    "Nullable(Int16)" -> PrimitiveClickhouseType(ClickHouseDataType.Int16, true, false),
    "Nullable(Int32)" -> PrimitiveClickhouseType(ClickHouseDataType.Int32, true, false),
    "Nullable(Int64)" -> PrimitiveClickhouseType(ClickHouseDataType.Int64, true, false),
    "Nullable(Int128)" -> PrimitiveClickhouseType(ClickHouseDataType.Int128, true, false),
    "Nullable(Int256)" -> PrimitiveClickhouseType(ClickHouseDataType.Int256, true, false),
    "Nullable(UInt8)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt8, true, false),
    "Nullable(UInt16)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt16, true, false),
    "Nullable(UInt32)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt32, true, false),
    "Nullable(UInt64)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt64, true, false),
    "Nullable(UInt128)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt128, true, false),
    "Nullable(UInt256)" -> PrimitiveClickhouseType(ClickHouseDataType.UInt256, true, false),
    "Nullable(String)" -> PrimitiveClickhouseType(ClickHouseDataType.String, true, false),
    "Nullable(Float32)" -> PrimitiveClickhouseType(ClickHouseDataType.Float32, true, false),
    "Nullable(Float64)" -> PrimitiveClickhouseType(ClickHouseDataType.Float64, true, false),
    "Nullable(Date)" -> PrimitiveClickhouseType(ClickHouseDataType.Date, true, false),
    "Nullable(Date32)" -> PrimitiveClickhouseType(ClickHouseDataType.Date32, true, false),
    "Nullable(DateTime)" -> ClickhouseDateTime(true, false)
  )

  property("Parsing Nullable of primitive types") {
    forAll(nullableOfPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val lowCardinalityOfNullablePrimitives = Table(
    "lowCardinalityOfNullablePrimitives",
    "LowCardinality(Nullable(Int8))" -> PrimitiveClickhouseType(ClickHouseDataType.Int8, true, true),
    "LowCardinality(Nullable(Int16))" -> PrimitiveClickhouseType(ClickHouseDataType.Int16, true, true),
    "LowCardinality(Nullable(Int32))" -> PrimitiveClickhouseType(ClickHouseDataType.Int32, true, true),
    "LowCardinality(Nullable(Int64))" -> PrimitiveClickhouseType(ClickHouseDataType.Int64, true, true),
    "LowCardinality(Nullable(Int128))" -> PrimitiveClickhouseType(ClickHouseDataType.Int128, true, true),
    "LowCardinality(Nullable(Int256))" -> PrimitiveClickhouseType(ClickHouseDataType.Int256, true, true),
    "LowCardinality(Nullable(UInt8))" -> PrimitiveClickhouseType(ClickHouseDataType.UInt8, true, true),
    "LowCardinality(Nullable(UInt16))" -> PrimitiveClickhouseType(ClickHouseDataType.UInt16, true, true),
    "LowCardinality(Nullable(UInt32))" -> PrimitiveClickhouseType(ClickHouseDataType.UInt32, true, true),
    "LowCardinality(Nullable(UInt64))" -> PrimitiveClickhouseType(ClickHouseDataType.UInt64, true, true),
    "LowCardinality(Nullable(UInt128))" -> PrimitiveClickhouseType(ClickHouseDataType.UInt128, true, true),
    "LowCardinality(Nullable(UInt256))" -> PrimitiveClickhouseType(ClickHouseDataType.UInt256, true, true),
    "LowCardinality(Nullable(String))" -> PrimitiveClickhouseType(ClickHouseDataType.String, true, true),
    "LowCardinality(Nullable(Float32))" -> PrimitiveClickhouseType(ClickHouseDataType.Float32, true, true),
    "LowCardinality(Nullable(Float64))" -> PrimitiveClickhouseType(ClickHouseDataType.Float64, true, true),
    "LowCardinality(Nullable(Date))" -> PrimitiveClickhouseType(ClickHouseDataType.Date, true, true),
    "LowCardinality(Nullable(Date32))" -> PrimitiveClickhouseType(ClickHouseDataType.Date32, true, true),
    "LowCardinality(Nullable(DateTime))" -> ClickhouseDateTime(true, true),
  )

  property("Parsing LowCardinality of Nullable of primitive types") {
    forAll(lowCardinalityOfNullablePrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfNullableOfPrimitives = Table(
    "arrayOfNullableOfPrimitives",
    "Array(Nullable(Int8))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int8, true, false)),
    "Array(Nullable(Int16))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int16, true, false)),
    "Array(Nullable(Int32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int32, true, false)),
    "Array(Nullable(Int64))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int64, true, false)),
    "Array(Nullable(Int128))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int128, true, false)),
    "Array(Nullable(Int256))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int256, true, false)),
    "Array(Nullable(UInt8))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt8, true, false)),
    "Array(Nullable(UInt16))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt16, true, false)),
    "Array(Nullable(UInt32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt32, true, false)),
    "Array(Nullable(UInt64))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt64, true, false)),
    "Array(Nullable(UInt128))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt128, true, false)),
    "Array(Nullable(UInt256))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt256, true, false)),
    "Array(Nullable(String))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.String, true, false)),
    "Array(Nullable(Float32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float32, true, false)),
    "Array(Nullable(Float64))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float64, true, false)),
    "Array(Nullable(Date))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date, true, false)),
    "Array(Nullable(Date32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date32, true, false)),
    "Array(Nullable(DateTime))" -> ClickhouseArray(ClickhouseDateTime(true, false)),
  )


  property("Parsing Array of Nullable of primitive types") {
    forAll(arrayOfNullableOfPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfLowCardinalityOfPrimitives = Table(
    "arrayOfLowCardinalityOfPrimitives",
    "Array(LowCardinality(Int8))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int8, false, true)),
    "Array(LowCardinality(Int16))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int16, false, true)),
    "Array(LowCardinality(Int32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int32, false, true)),
    "Array(LowCardinality(Int64))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int64, false, true)),
    "Array(LowCardinality(Int128))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int128, false, true)),
    "Array(LowCardinality(Int256))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int256, false, true)),
    "Array(LowCardinality(UInt8))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt8, false, true)),
    "Array(LowCardinality(UInt16))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt16, false, true)),
    "Array(LowCardinality(UInt32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt32, false, true)),
    "Array(LowCardinality(UInt64))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt64, false, true)),
    "Array(LowCardinality(UInt128))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt128, false, true)),
    "Array(LowCardinality(UInt256))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt256, false, true)),
    "Array(LowCardinality(String))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.String, false, true)),
    "Array(LowCardinality(Float32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float32, false, true)),
    "Array(LowCardinality(Float64))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float64, false, true)),
    "Array(LowCardinality(Date))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date, false, true)),
    "Array(LowCardinality(Date32))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date32, false, true)),
    "Array(LowCardinality(DateTime))" -> ClickhouseArray(ClickhouseDateTime(false, true)),
  )

  property("Parsing Array of LowCardinality of primitive types") {
    forAll(arrayOfLowCardinalityOfPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfLowCardinalityOfNullableOfPrimitives = Table(
    "arrayOfLowCardinalityOfNullableOfPrimitives",
    "Array(LowCardinality(Nullable(Int8)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int8, true, true)),
    "Array(LowCardinality(Nullable(Int16)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int16, true, true)),
    "Array(LowCardinality(Nullable(Int32)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int32, true, true)),
    "Array(LowCardinality(Nullable(Int64)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int64, true, true)),
    "Array(LowCardinality(Nullable(Int128)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int128, true, true)),
    "Array(LowCardinality(Nullable(Int256)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Int256, true, true)),
    "Array(LowCardinality(Nullable(UInt8)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt8, true, true)),
    "Array(LowCardinality(Nullable(UInt16)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt16, true, true)),
    "Array(LowCardinality(Nullable(UInt32)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt32, true, true)),
    "Array(LowCardinality(Nullable(UInt64)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt64, true, true)),
    "Array(LowCardinality(Nullable(UInt128)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt128, true, true)),
    "Array(LowCardinality(Nullable(UInt256)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt256, true, true)),
    "Array(LowCardinality(Nullable(String)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.String, true, true)),
    "Array(LowCardinality(Nullable(Float32)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float32, true, true)),
    "Array(LowCardinality(Nullable(Float64)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Float64, true, true)),
    "Array(LowCardinality(Nullable(Date)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date, true, true)),
    "Array(LowCardinality(Nullable(Date32)))" -> ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.Date32, true, true)),
    "Array(LowCardinality(Nullable(DateTime)))" -> ClickhouseArray(ClickhouseDateTime(true, true)),
  )

  property("Parsing Array of LowCardinality of Nullable of primitive types") {
    forAll(arrayOfLowCardinalityOfPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val decimals = Table(
    "decimals",
    "Decimal(4, 5)" -> ClickhouseDecimal(4 ,5, false),
    "Nullable(Decimal(8, 10))" -> ClickhouseDecimal(8, 10, true),
    "Array(Nullable(Decimal(4, 5)))" -> ClickhouseArray(ClickhouseDecimal(4, 5, true)),
  )

  property("Parsing various of Decimals") {
    forAll(decimals) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val dates = Table(
    "dates",
    "Date" -> PrimitiveClickhouseType(ClickHouseDataType.Date, false, false),
    "Date32" -> PrimitiveClickhouseType(ClickHouseDataType.Date32, false, false),
    "DateTime" -> ClickhouseDateTime(false, false),
    "DateTime64(8)" -> ClickhouseDateTime64(8, false),
    "Nullable(DateTime)" -> ClickhouseDateTime(true, false),
    "LowCardinality(DateTime('Europe/Moscow'))" -> ClickhouseDateTime(false, true),
    "Nullable(DateTime('Europe/Moscow'))" -> ClickhouseDateTime(true, false),
  )

  property("Parsing various DateTimes") {
    forAll(dates) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }
}
