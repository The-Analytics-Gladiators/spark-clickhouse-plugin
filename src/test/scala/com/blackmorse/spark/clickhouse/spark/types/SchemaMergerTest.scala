package com.blackmorse.spark.clickhouse.spark.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseInt32, ClickhouseString}
import com.blackmorse.spark.clickhouse.sql.types.{ClickhouseDateTime, ClickhouseField}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class SchemaMergerTest extends AnyFunSuiteLike with Matchers {
  test("Should merge with two fields") {
    val intSparkField = StructField(name = "field1", IntegerType)
    val stringSparkField = StructField(name = "field2", StringType)
    val schema = StructType(List(intSparkField, stringSparkField))

    val stringClickhouseField = ClickhouseField("field2", ClickhouseString(nullable = true, lowCardinality = true))
    val intClickhouseField = ClickhouseField("field1", ClickhouseInt32(nullable = true, lowCardinality = true))
    val clickhouseFields = Seq(stringClickhouseField, intClickhouseField)

    val result = SchemaMerger.mergeSchemas(schema, clickhouseFields)

    result should contain ((intSparkField, intClickhouseField))
    result should contain ((stringSparkField, stringClickhouseField))
  }

  test("Should merge if clickhouse fields more than spark fields") {
    def structField(i: Int) = StructField(s"field$i", IntegerType)
    def clickhouseField(i: Int) = ClickhouseField(s"field$i", ClickhouseInt32(nullable = true, lowCardinality = true))

    val schema = StructType((1 to 10) map structField)
    val clickhouseFields = (1 to 20) map clickhouseField

    val result = SchemaMerger.mergeSchemas(schema, clickhouseFields)

    result should contain allElementsOf ((1 to 10) map (i => (structField(i), clickhouseField(i))))
    result.size should be (10)
  }

  test("Should fail if at least on  spark field do not exist at clickhouse") {
    val schema = StructType(List(StructField("field1", TimestampType), StructField("field2", StringType)))
    val clickhouseFields = Seq(ClickhouseField("field1", ClickhouseDateTime(true, true)))

    assertThrows[Exception] {
      SchemaMerger.mergeSchemas(schema, clickhouseFields)
    }
  }
}
