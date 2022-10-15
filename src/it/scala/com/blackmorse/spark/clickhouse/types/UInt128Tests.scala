package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.arrays.BigIntArraySupport
import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt128
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{ByteType, DecimalType, IntegerType, LongType, ShortType}
import org.scalatest.flatspec.AnyFlatSpec

class UInt128Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UUInt128" should "be supported" in {
    testPrimitiveAndArray(new ClickhouseUInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toString),
        Seq("0", "340282366920938463463374607431768211455")
      ),
      rowConverter = row => row.getString(0)
    )
  }

  "ByteType" should "be supported by UInt128" in {
    testPrimitiveAndArray(new ClickhouseUInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq(0.toByte, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = _.asInstanceOf[Byte].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "ShortType" should "be supported by UInt128" in {
    testPrimitiveAndArray(new ClickhouseUInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(0.toShort, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = _.asInstanceOf[Short].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "IntegerType" should "be supported by UInt128" in {
    testPrimitiveAndArray(new ClickhouseUInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        1 to 100,
        Seq(0, Int.MaxValue)
      ),
      forceSparkType = IntegerType,
      convertToOriginalType = _.asInstanceOf[Int].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "LongType" should "be supported by UInt128" in {
    testPrimitiveAndArray(new ClickhouseUInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toLong),
        Seq(0.toLong, Long.MaxValue)
      ),
      forceSparkType = LongType,
      convertToOriginalType = _.asInstanceOf[Long].toString,
      rowConverter = row => row.getString(0)
    )
  }

  "DecimalType" should "be supported by UInt128" in {
    testPrimitiveAndArray(new ClickhouseUInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (i => new java.math.BigDecimal(i.toString)),
        Seq(new java.math.BigDecimal("0"), new java.math.BigDecimal("9" * 38))
      ),
      forceSparkType = DecimalType(38, 0),
      convertToOriginalType = _.asInstanceOf[java.math.BigDecimal].toString,
      rowConverter = row => row.getString(0)
    )
  }
}
