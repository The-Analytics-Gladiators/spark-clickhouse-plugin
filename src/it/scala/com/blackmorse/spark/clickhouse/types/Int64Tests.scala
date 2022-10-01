package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt64
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{ByteType, IntegerType, ShortType}
import org.scalatest.flatspec.AnyFlatSpec

class Int64Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int64" should "be supported" in {
    testPrimitiveAndArray(ClickhouseInt64(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toLong),
        Seq(Long.MinValue, Long.MaxValue)
      ),
      rowConverter = row => row.getLong(0)
    )
  }

  "ByteType" should "be supported by Int64" in {
    testPrimitiveAndArray(ClickhouseInt64(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq(Byte.MinValue, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = _.asInstanceOf[Byte].toLong,
      rowConverter = row => row.getLong(0)
    )
  }

  "ShortType" should "be supported by Int64" in {
    testPrimitiveAndArray(ClickhouseInt64(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(Short.MinValue, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = _.asInstanceOf[Short].toLong,
      rowConverter = row => row.getLong(0)
    )
  }

  "IntegerType" should "be supported by Int64" in {
    testPrimitiveAndArray(ClickhouseInt64(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100),
        Seq(Int.MinValue, Int.MaxValue)
      ),
      forceSparkType = IntegerType,
      convertToOriginalType = _.asInstanceOf[Int].toLong,
      rowConverter = row => row.getLong(0)
    )
  }
}
