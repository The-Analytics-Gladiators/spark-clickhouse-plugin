package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt32
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{ByteType, IntegerType, ShortType}
import org.scalatest.flatspec.AnyFlatSpec

class UInt32Tests  extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt32" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt32(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toLong),
        Seq(0, 4294967295L)
      ),
      rowConverter = row => row.getLong(0)
    )
  }

  "ByteType" should "be supported by UInt16" in {
    testPrimitiveAndArray(ClickhouseUInt32(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq(0.toByte, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = _.asInstanceOf[Byte].toLong,
      rowConverter = row => row.getLong(0)
    )
  }

  "ShortType" should "be supported by UInt16" in {
    testPrimitiveAndArray(ClickhouseUInt32(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(0.toShort, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = _.asInstanceOf[Short].toLong,
      rowConverter = row => row.getLong(0)
    )
  }

  "Integer" should "be supported by UInt16" in {
    testPrimitiveAndArray(ClickhouseUInt32(nullable = false, lowCardinality = false))(
      cases = Seq(
        1 to 100,
        Seq(0, Int.MaxValue)
      ),
      forceSparkType = IntegerType,
      convertToOriginalType = _.asInstanceOf[Int].toLong,
      rowConverter = row => row.getLong(0)
    )
  }
}
