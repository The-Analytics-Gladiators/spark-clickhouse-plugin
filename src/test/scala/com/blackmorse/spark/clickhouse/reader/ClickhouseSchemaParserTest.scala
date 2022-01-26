package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
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
          ClickhouseField("a", PrimitiveClickhouseType(ClickHouseDataType.Int32, nullable = false, lowCardinality = false)),
          ClickhouseField("b", PrimitiveClickhouseType(ClickHouseDataType.String, nullable = false, lowCardinality = true)),
          ClickhouseField("c", PrimitiveClickhouseType(ClickHouseDataType.Float64, nullable = true, lowCardinality = false)),
          ClickhouseField("d", PrimitiveClickhouseType(ClickHouseDataType.String, nullable = true, lowCardinality = true)),
          ClickhouseField("e", ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.DateTime, nullable = false, lowCardinality = false))),
          ClickhouseField("f", ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt256, nullable = true, lowCardinality = false))),
          ClickhouseField("g", ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt8, nullable = false, lowCardinality = true))),
          ClickhouseField("h", ClickhouseArray(PrimitiveClickhouseType(ClickHouseDataType.UInt64, nullable = true, lowCardinality = true))),
      ))
    }
  }
}
