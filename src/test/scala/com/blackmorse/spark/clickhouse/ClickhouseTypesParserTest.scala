package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.reader.ClickhouseTypesParser
import com.blackmorse.spark.clickhouse.sql.types._
import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseBoolean, ClickhouseDate, ClickhouseDate32, ClickhouseFloat32, ClickhouseFloat64, ClickhouseInt128, ClickhouseInt16, ClickhouseInt256, ClickhouseInt32, ClickhouseInt64, ClickhouseInt8, ClickhouseString, ClickhouseUInt128, ClickhouseUInt16, ClickhouseUInt256, ClickhouseUInt32, ClickhouseUInt64, ClickhouseUInt8}
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.propspec.AnyPropSpec

class ClickhouseTypesParserTest extends AnyPropSpec
  with TableDrivenPropertyChecks
  with should.Matchers {

  val primitives = Table(
    "primitives",
    "Int8" -> ClickhouseInt8(false, false),
    "Int16" -> ClickhouseInt16(false, false),
    "Int32" -> ClickhouseInt32(false, false),
    "Int64" -> ClickhouseInt64(false, false),
    "Int128" -> ClickhouseInt128(false, false),
    "Int256" -> ClickhouseInt256(false, false),
    "UInt8" -> ClickhouseUInt8(false, false),
    "UInt16" -> ClickhouseUInt16(false, false),
    "UInt32" -> ClickhouseUInt32(false, false),
    "UInt64" -> ClickhouseUInt64(false, false),
    "UInt128" -> ClickhouseUInt128(false, false),
    "UInt256" -> ClickhouseUInt256(false, false),
    "String" -> ClickhouseString(false, false),
    "Float32" -> ClickhouseFloat32(false, false),
    "Float64" -> ClickhouseFloat64(false, false),
    "Date" -> ClickhouseDate(false, false),
    "Date32" -> ClickhouseDate32(false, false),
    "DateTime" -> ClickhouseDateTime(false, false),
    "Bool" -> ClickhouseBoolean(false, false)
  )

  property("Parsing simple primitive types") {
    forAll(primitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfPrimitives = Table(
    "arrayOfPrimitives",
    "Array(Int8)" -> ClickhouseArray(ClickhouseInt8(false, false)),
    "Array(Int16)" -> ClickhouseArray(ClickhouseInt16(false, false)),
    "Array(Int32)" -> ClickhouseArray(ClickhouseInt32(false, false)),
    "Array(Int64)" -> ClickhouseArray(ClickhouseInt64(false, false)),
    "Array(Int128)" -> ClickhouseArray(ClickhouseInt128(false, false)),
    "Array(Int256)" -> ClickhouseArray(ClickhouseInt256(false, false)),
    "Array(UInt8)" -> ClickhouseArray(ClickhouseUInt8(false, false)),
    "Array(UInt16)" -> ClickhouseArray(ClickhouseUInt16(false, false)),
    "Array(UInt32)" -> ClickhouseArray(ClickhouseUInt32(false, false)),
    "Array(UInt64)" -> ClickhouseArray(ClickhouseUInt64(false, false)),
    "Array(UInt128)" -> ClickhouseArray(ClickhouseUInt128(false, false)),
    "Array(UInt256)" -> ClickhouseArray(ClickhouseUInt256(false, false)),
    "Array(String)" -> ClickhouseArray(ClickhouseString(false, false)),
    "Array(Float32)" -> ClickhouseArray(ClickhouseFloat32(false, false)),
    "Array(Float64)" -> ClickhouseArray(ClickhouseFloat64(false, false)),
    "Array(Date)" -> ClickhouseArray(ClickhouseDate(false, false)),
    "Array(Date32)" -> ClickhouseArray(ClickhouseDate32(false, false)),
    "Array(DateTime)" -> ClickhouseArray(ClickhouseDateTime(false, false)),
    "Array(Bool)" -> ClickhouseArray(ClickhouseBoolean(false, false))
  )

  property("Parsing array of simple primitive types") {
    forAll(arrayOfPrimitives) { case(typ, expectedResult) =>
    ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val lowCardinalityofPrimitives = Table(
    "lowCardinalityOfPrimitives",
    "LowCardinality(Int8)" -> ClickhouseInt8(false, true),
    "LowCardinality(Int16)" -> ClickhouseInt16(false, true),
    "LowCardinality(Int32)" -> ClickhouseInt32(false, true),
    "LowCardinality(Int64)" -> ClickhouseInt64(false, true),
    "LowCardinality(Int128)" -> ClickhouseInt128(false, true),
    "LowCardinality(Int256)" -> ClickhouseInt256(false, true),
    "LowCardinality(UInt8)" -> ClickhouseUInt8(false, true),
    "LowCardinality(UInt16)" -> ClickhouseUInt16(false, true),
    "LowCardinality(UInt32)" -> ClickhouseUInt32(false, true),
    "LowCardinality(UInt64)" -> ClickhouseUInt64(false, true),
    "LowCardinality(UInt128)" -> ClickhouseUInt128(false, true),
    "LowCardinality(UInt256)" -> ClickhouseUInt256(false, true),
    "LowCardinality(String)" -> ClickhouseString(false, true),
    "LowCardinality(Float32)" -> ClickhouseFloat32(false, true),
    "LowCardinality(Float64)" -> ClickhouseFloat64(false, true),
    "LowCardinality(Date)" -> ClickhouseDate(false, true),
    "LowCardinality(Date32)" -> ClickhouseDate32(false, true),
    "LowCardinality(DateTime)" -> ClickhouseDateTime(false, true),
    "LowCardinality(Bool)" -> ClickhouseBoolean(false, true)
  )

  property("Parsing LowCardinality of primitive types") {
    forAll(lowCardinalityofPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val nullableOfPrimitives = Table(
    "nullableOfPrimitives",
    "Nullable(Int8)" -> ClickhouseInt8(true, false),
    "Nullable(Int16)" -> ClickhouseInt16(true, false),
    "Nullable(Int32)" -> ClickhouseInt32(true, false),
    "Nullable(Int64)" -> ClickhouseInt64(true, false),
    "Nullable(Int128)" -> ClickhouseInt128(true, false),
    "Nullable(Int256)" -> ClickhouseInt256(true, false),
    "Nullable(UInt8)" -> ClickhouseUInt8(true, false),
    "Nullable(UInt16)" -> ClickhouseUInt16(true, false),
    "Nullable(UInt32)" -> ClickhouseUInt32(true, false),
    "Nullable(UInt64)" -> ClickhouseUInt64(true, false),
    "Nullable(UInt128)" -> ClickhouseUInt128(true, false),
    "Nullable(UInt256)" -> ClickhouseUInt256(true, false),
    "Nullable(String)" -> ClickhouseString(true, false),
    "Nullable(Float32)" -> ClickhouseFloat32(true, false),
    "Nullable(Float64)" -> ClickhouseFloat64(true, false),
    "Nullable(Date)" -> ClickhouseDate(true, false),
    "Nullable(Date32)" -> ClickhouseDate32(true, false),
    "Nullable(DateTime)" -> ClickhouseDateTime(true, false),
    "Nullable(Bool)" -> ClickhouseBoolean(true, false)
  )

  property("Parsing Nullable of primitive types") {
    forAll(nullableOfPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val lowCardinalityOfNullablePrimitives = Table(
    "lowCardinalityOfNullablePrimitives",
    "LowCardinality(Nullable(Int8))" -> ClickhouseInt8(true, true),
    "LowCardinality(Nullable(Int16))" -> ClickhouseInt16(true, true),
    "LowCardinality(Nullable(Int32))" -> ClickhouseInt32(true, true),
    "LowCardinality(Nullable(Int64))" -> ClickhouseInt64(true, true),
    "LowCardinality(Nullable(Int128))" -> ClickhouseInt128(true, true),
    "LowCardinality(Nullable(Int256))" -> ClickhouseInt256(true, true),
    "LowCardinality(Nullable(UInt8))" -> ClickhouseUInt8(true, true),
    "LowCardinality(Nullable(UInt16))" -> ClickhouseUInt16(true, true),
    "LowCardinality(Nullable(UInt32))" -> ClickhouseUInt32(true, true),
    "LowCardinality(Nullable(UInt64))" -> ClickhouseUInt64(true, true),
    "LowCardinality(Nullable(UInt128))" -> ClickhouseUInt128(true, true),
    "LowCardinality(Nullable(UInt256))" -> ClickhouseUInt256(true, true),
    "LowCardinality(Nullable(String))" -> ClickhouseString(true, true),
    "LowCardinality(Nullable(Float32))" -> ClickhouseFloat32(true, true),
    "LowCardinality(Nullable(Float64))" -> ClickhouseFloat64(true, true),
    "LowCardinality(Nullable(Date))" -> ClickhouseDate(true, true),
    "LowCardinality(Nullable(Date32))" -> ClickhouseDate32(true, true),
    "LowCardinality(Nullable(DateTime))" -> ClickhouseDateTime(true, true),
    "LowCardinality(Nullable(Bool))" -> ClickhouseBoolean(true, true)
  )

  property("Parsing LowCardinality of Nullable of primitive types") {
    forAll(lowCardinalityOfNullablePrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfNullableOfPrimitives = Table(
    "arrayOfNullableOfPrimitives",
    "Array(Nullable(Int8))" -> ClickhouseArray(ClickhouseInt8(true, false)),
    "Array(Nullable(Int16))" -> ClickhouseArray(ClickhouseInt16(true, false)),
    "Array(Nullable(Int32))" -> ClickhouseArray(ClickhouseInt32(true, false)),
    "Array(Nullable(Int64))" -> ClickhouseArray(ClickhouseInt64(true, false)),
    "Array(Nullable(Int128))" -> ClickhouseArray(ClickhouseInt128(true, false)),
    "Array(Nullable(Int256))" -> ClickhouseArray(ClickhouseInt256(true, false)),
    "Array(Nullable(UInt8))" -> ClickhouseArray(ClickhouseUInt8(true, false)),
    "Array(Nullable(UInt16))" -> ClickhouseArray(ClickhouseUInt16(true, false)),
    "Array(Nullable(UInt32))" -> ClickhouseArray(ClickhouseUInt32(true, false)),
    "Array(Nullable(UInt64))" -> ClickhouseArray(ClickhouseUInt64(true, false)),
    "Array(Nullable(UInt128))" -> ClickhouseArray(ClickhouseUInt128(true, false)),
    "Array(Nullable(UInt256))" -> ClickhouseArray(ClickhouseUInt256(true, false)),
    "Array(Nullable(String))" -> ClickhouseArray(ClickhouseString(true, false)),
    "Array(Nullable(Float32))" -> ClickhouseArray(ClickhouseFloat32(true, false)),
    "Array(Nullable(Float64))" -> ClickhouseArray(ClickhouseFloat64(true, false)),
    "Array(Nullable(Date))" -> ClickhouseArray(ClickhouseDate(true, false)),
    "Array(Nullable(Date32))" -> ClickhouseArray(ClickhouseDate32(true, false)),
    "Array(Nullable(DateTime))" -> ClickhouseArray(ClickhouseDateTime(true, false)),
    "Array(Nullable(Bool))" -> ClickhouseArray(ClickhouseBoolean(true, false))
  )


  property("Parsing Array of Nullable of primitive types") {
    forAll(arrayOfNullableOfPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfLowCardinalityOfPrimitives = Table(
    "arrayOfLowCardinalityOfPrimitives",
    "Array(LowCardinality(Int8))" -> ClickhouseArray(ClickhouseInt8(false, true)),
    "Array(LowCardinality(Int16))" -> ClickhouseArray(ClickhouseInt16(false, true)),
    "Array(LowCardinality(Int32))" -> ClickhouseArray(ClickhouseInt32(false, true)),
    "Array(LowCardinality(Int64))" -> ClickhouseArray(ClickhouseInt64(false, true)),
    "Array(LowCardinality(Int128))" -> ClickhouseArray(ClickhouseInt128(false, true)),
    "Array(LowCardinality(Int256))" -> ClickhouseArray(ClickhouseInt256(false, true)),
    "Array(LowCardinality(UInt8))" -> ClickhouseArray(ClickhouseUInt8(false, true)),
    "Array(LowCardinality(UInt16))" -> ClickhouseArray(ClickhouseUInt16(false, true)),
    "Array(LowCardinality(UInt32))" -> ClickhouseArray(ClickhouseUInt32(false, true)),
    "Array(LowCardinality(UInt64))" -> ClickhouseArray(ClickhouseUInt64(false, true)),
    "Array(LowCardinality(UInt128))" -> ClickhouseArray(ClickhouseUInt128(false, true)),
    "Array(LowCardinality(UInt256))" -> ClickhouseArray(ClickhouseUInt256(false, true)),
    "Array(LowCardinality(String))" -> ClickhouseArray(ClickhouseString(false, true)),
    "Array(LowCardinality(Float32))" -> ClickhouseArray(ClickhouseFloat32(false, true)),
    "Array(LowCardinality(Float64))" -> ClickhouseArray(ClickhouseFloat64(false, true)),
    "Array(LowCardinality(Date))" -> ClickhouseArray(ClickhouseDate(false, true)),
    "Array(LowCardinality(Date32))" -> ClickhouseArray(ClickhouseDate32(false, true)),
    "Array(LowCardinality(DateTime))" -> ClickhouseArray(ClickhouseDateTime(false, true)),
    "Array(LowCardinality(Bool))" -> ClickhouseArray(ClickhouseBoolean(false, true))
  )

  property("Parsing Array of LowCardinality of primitive types") {
    forAll(arrayOfLowCardinalityOfPrimitives) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be (expectedResult)
    }
  }

  val arrayOfLowCardinalityOfNullableOfPrimitives = Table(
    "arrayOfLowCardinalityOfNullableOfPrimitives",
    "Array(LowCardinality(Nullable(Int8)))" -> ClickhouseArray(ClickhouseInt8(true, true)),
    "Array(LowCardinality(Nullable(Int16)))" -> ClickhouseArray(ClickhouseInt16(true, true)),
    "Array(LowCardinality(Nullable(Int32)))" -> ClickhouseArray(ClickhouseInt32(true, true)),
    "Array(LowCardinality(Nullable(Int64)))" -> ClickhouseArray(ClickhouseInt64(true, true)),
    "Array(LowCardinality(Nullable(Int128)))" -> ClickhouseArray(ClickhouseInt128(true, true)),
    "Array(LowCardinality(Nullable(Int256)))" -> ClickhouseArray(ClickhouseInt256(true, true)),
    "Array(LowCardinality(Nullable(UInt8)))" -> ClickhouseArray(ClickhouseUInt8(true, true)),
    "Array(LowCardinality(Nullable(UInt16)))" -> ClickhouseArray(ClickhouseUInt16(true, true)),
    "Array(LowCardinality(Nullable(UInt32)))" -> ClickhouseArray(ClickhouseUInt32(true, true)),
    "Array(LowCardinality(Nullable(UInt64)))" -> ClickhouseArray(ClickhouseUInt64(true, true)),
    "Array(LowCardinality(Nullable(UInt128)))" -> ClickhouseArray(ClickhouseUInt128(true, true)),
    "Array(LowCardinality(Nullable(UInt256)))" -> ClickhouseArray(ClickhouseUInt256(true, true)),
    "Array(LowCardinality(Nullable(String)))" -> ClickhouseArray(ClickhouseString(true, true)),
    "Array(LowCardinality(Nullable(Float32)))" -> ClickhouseArray(ClickhouseFloat32(true, true)),
    "Array(LowCardinality(Nullable(Float64)))" -> ClickhouseArray(ClickhouseFloat64(true, true)),
    "Array(LowCardinality(Nullable(Date)))" -> ClickhouseArray(ClickhouseDate(true, true)),
    "Array(LowCardinality(Nullable(Date32)))" -> ClickhouseArray(ClickhouseDate32(true, true)),
    "Array(LowCardinality(Nullable(DateTime)))" -> ClickhouseArray(ClickhouseDateTime(true, true)),
    "Array(LowCardinality(Nullable(Bool)))" -> ClickhouseArray(ClickhouseBoolean(true, true))
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
    "Date" -> ClickhouseDate(false, false),
    "Date32" -> ClickhouseDate32(false, false),
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

  val fixedStrings = Table(
    "fixedStrings",
    "FixedString(3)" -> ClickhouseFixedString(false, false, 3),
    "Nullable(FixedString(3))" -> ClickhouseFixedString(true, false, 3),
    "LowCardinality(FixedString(3))" -> ClickhouseFixedString(false, true, 3),
    "Array(Nullable(FixedString(3)))" -> ClickhouseArray(ClickhouseFixedString(true, false, 3)),
  )

  property("Parsing various of Fixed Strings") {
    forAll(fixedStrings) { case (typ, expectedResult) =>
      ClickhouseTypesParser.parseType(typ) should be(expectedResult)
    }
  }
}
