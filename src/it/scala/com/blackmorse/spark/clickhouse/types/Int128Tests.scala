package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt128
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{ByteType, DecimalType, IntegerType, LongType, ShortType}
import org.scalatest.flatspec.AnyFlatSpec

class Int128Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int128" should "be supported" in {
    testPrimitiveAndArray(ClickhouseInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toString),
        Seq("-170141183460469231731687303715884105728", "170141183460469231731687303715884105727")
      ),
      rowConverter = row => row.getString(0)
    )
  }

  "ByteType" should "be supported by Int128" in {
    testPrimitiveAndArray(ClickhouseInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq(Byte.MinValue, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = _.asInstanceOf[Byte].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "ShortType" should "be supported by Int128" in {
    testPrimitiveAndArray(ClickhouseInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(Short.MinValue, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = _.asInstanceOf[Short].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "IntegerType" should "be supported by Int128" in {
    testPrimitiveAndArray(ClickhouseInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        1 to 100,
        Seq(Int.MinValue, Int.MaxValue)
      ),
      forceSparkType = IntegerType,
      convertToOriginalType = _.asInstanceOf[Int].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "LongType" should "be supported by Int128" in {
    testPrimitiveAndArray(ClickhouseInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toLong),
        Seq(Long.MinValue, Long.MaxValue)
      ),
      forceSparkType = LongType,
      convertToOriginalType = _.asInstanceOf[Long].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "DecimalType" should "be supported by Int128" in {
    testPrimitiveAndArray(ClickhouseInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (i => new java.math.BigDecimal(i.toString)),
        Seq(new java.math.BigDecimal("-" + "9" * 37), new java.math.BigDecimal("9" * 38))
      ),
      forceSparkType = DecimalType(38, 0),
      convertToOriginalType = _.asInstanceOf[java.math.BigDecimal].toString,
      rowConverter = row => row.getString(0)
    )
  }
}
