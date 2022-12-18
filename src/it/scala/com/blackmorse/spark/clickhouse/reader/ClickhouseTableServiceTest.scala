package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.blackmorse.spark.clickhouse.sql.types._
import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseDate, ClickhouseFloat64, ClickhouseInt128, ClickhouseInt16, ClickhouseInt256, ClickhouseInt32, ClickhouseInt64, ClickhouseInt8, ClickhouseString, ClickhouseUInt128, ClickhouseUInt16, ClickhouseUInt256, ClickhouseUInt32, ClickhouseUInt64, ClickhouseUInt8}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.services.ClickhouseTableService

class ClickhouseTableServiceTest extends AnyFlatSpec with Matchers {
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

      val parsedTable = ClickhouseTableService.fetchFieldsAndEngine(s"jdbc:clickhouse://$shard1Replica1", "default.test_table")
      val fields = parsedTable.get.fields
      parsedTable.get.engine should be("MergeTree")

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
      val parsedTable = ClickhouseTableService.fetchFieldsAndEngine(s"jdbc:clickhouse://$shard1Replica1", "default.test_table")
      val fields = parsedTable.get.fields
      parsedTable.get.engine should be("MergeTree")

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
      val parsedTable = ClickhouseTableService.fetchFieldsAndEngine(s"jdbc:clickhouse://$shard1Replica1", "default.test_table")
      val fields = parsedTable.get.fields
      parsedTable.get.engine should be("MergeTree")

      assert(fields equals Seq(
        ClickhouseField("a", ClickhouseDate(nullable = false, lowCardinality = false)),
//        ClickhouseField("b", PrimitiveClickhouseType(ClickHouseDataType.Date32, nullable = false, lowCardinality = false)),
        ClickhouseField("c", ClickhouseDateTime(nullable = false, lowCardinality = false)),
        ClickhouseField("d", ClickhouseDateTime64(8, nullable = false)),
        ClickhouseField("e", ClickhouseDateTime(nullable = true, lowCardinality = false))
      ))
    }
  }

  "ClickhouseSchemaParser" should "recognize (U)Int{8-256} types" in {
    withTable(Seq(
      "a UInt8",
      "b Int8",
      "c Int16",
      "d UInt16",
      "e Int32",
      "f UInt32",
      "g Int64",
      "h UInt64",
      "j Int128",
      "k UInt128",
      "l Int256",
      "m UInt256"
    ), "a") {
      val parsedTable = ClickhouseTableService.fetchFieldsAndEngine(s"jdbc:clickhouse://$shard1Replica1", "default.test_table")
      val fields = parsedTable.get.fields
      parsedTable.get.engine should be("MergeTree")

      assert(fields equals Seq(
        ClickhouseField("a", ClickhouseUInt8(nullable = false, lowCardinality = false)),
        ClickhouseField("b", ClickhouseInt8(nullable = false, lowCardinality = false)),
        ClickhouseField("c", ClickhouseInt16(nullable = false, lowCardinality = false)),
        ClickhouseField("d", ClickhouseUInt16(nullable = false, lowCardinality = false)),
        ClickhouseField("e", ClickhouseInt32(nullable = false, lowCardinality = false)),
        ClickhouseField("f", ClickhouseUInt32(nullable = false, lowCardinality = false)),
        ClickhouseField("g", ClickhouseInt64(nullable = false, lowCardinality = false)),
        ClickhouseField("h", ClickhouseUInt64(nullable = false, lowCardinality = false)),
        ClickhouseField("j", ClickhouseInt128(nullable = false, lowCardinality = false)),
        ClickhouseField("k", ClickhouseUInt128(nullable = false, lowCardinality = false)),
        ClickhouseField("l", ClickhouseInt256(nullable = false, lowCardinality = false)),
        ClickhouseField("m", ClickhouseUInt256(nullable = false, lowCardinality = false)),
      ))
    }
  }
}
