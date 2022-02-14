package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.blackmorse.spark.clickhouse.sql.types._
import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseDate, ClickhouseFloat64, ClickhouseInt32, ClickhouseString, ClickhouseUInt256, ClickhouseUInt64, ClickhouseUInt8}
import com.clickhouse.client.ClickHouseDataType
import org.scalatest.flatspec.AnyFlatSpec

class ClickhouseSchemaParserTest extends AnyFlatSpec {
  "ClickhouseSchemaParser" should "parse schema with Nullables, Arrays and LowCardinalities" in {
    withTable(Seq(
      "a Int32",
      "b LowCardinality(String)",
      "c Nullable(Float64)",
      "d LowCardinality(Nullable(String))",
      "e Array(DateTime)",
      "f Array(Nullable(UInt256))",
      "g Array(LowCardinality(UInt8))",
      "h Array(LowCardinality(Nullable(UInt64)))"), "a") {

      val fields = ClickhouseSchemaParser.parseTable("jdbc:clickhouse://localhost:8123", "default.test_table")

      assert(fields equals Seq(
          ClickhouseField("a", ClickhouseInt32(nullable = false, lowCardinality = false)),
          ClickhouseField("b", ClickhouseString(nullable = false, lowCardinality = true)),
          ClickhouseField("c", ClickhouseFloat64(nullable = true, lowCardinality = false)),
          ClickhouseField("d", ClickhouseString(nullable = true, lowCardinality = true)),
          ClickhouseField("e", ClickhouseArray(ClickhouseDateTime(nullable = false, lowCardinality = false))),
          ClickhouseField("f", ClickhouseArray(ClickhouseUInt256(nullable = true, lowCardinality = false))),
          ClickhouseField("g", ClickhouseArray(ClickhouseUInt8(nullable = false, lowCardinality = true))),
          ClickhouseField("h", ClickhouseArray(ClickhouseUInt64(nullable = true, lowCardinality = true))),
      ))
    }
  }

  "ClickhouseSchemaParser" should "parse schema with Decimals" in {
    withTable(Seq(
      "a Decimal(3, 2)",
      "b Decimal32(5)",
      "c Decimal64(8)",
      "d Decimal128(12)",
    ), "a") {
      val fields = ClickhouseSchemaParser.parseTable("jdbc:clickhouse://localhost:8123", "default.test_table")

      assert(fields equals Seq(
        ClickhouseField("a", ClickhouseDecimal(3, 2, nullable = false)),
        ClickhouseField("b", ClickhouseDecimal(9, 5, nullable = false)),
        ClickhouseField("c", ClickhouseDecimal(18, 8, nullable = false)),
        ClickhouseField("d", ClickhouseDecimal(38, 12, nullable = false)),
      ))
    }
  }

  "ClickhouseSchemaParser" should "parse Date and DateTimes" in {
    withTable(Seq(
      "a Date",
//      "b Date32", TODO CH version after 21.8
      "c DateTime",
      "d DateTime64(8)",
      "e Nullable(DateTime('UTC'))"
    ), "a") {
      val fields = ClickhouseSchemaParser.parseTable("jdbc:clickhouse://localhost:8123", "default.test_table")

      assert(fields equals Seq(
        ClickhouseField("a", ClickhouseDate(nullable = false, lowCardinality = false)),
//        ClickhouseField("b", PrimitiveClickhouseType(ClickHouseDataType.Date32, nullable = false, lowCardinality = false)),
        ClickhouseField("c", ClickhouseDateTime(nullable = false, lowCardinality = false)),
        ClickhouseField("d", ClickhouseDateTime64(8, nullable = false)),
        ClickhouseField("e", ClickhouseDateTime(nullable = true, lowCardinality = false))
      ))
    }
  }
}
