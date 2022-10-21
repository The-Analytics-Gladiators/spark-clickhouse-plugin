package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseBoolean, ClickhouseInt8}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}

class BoolTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Bool" should "be supported" in {
    testPrimitiveAndArray(clickhouseType = ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        Seq(true, false)
      ),
      rowConverter = row => row.getBoolean(0)
    )
  }

  "Bool" should "work with ByteType" in {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        (0 to 100) map (_.toByte),
        Seq(Byte.MinValue, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = b => if (b.asInstanceOf[Byte] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }

  "Bool" should "work with ShortType" in {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        (-100 to 100) map (_.toShort),
        Seq(Short.MinValue, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = b => if (b.asInstanceOf[Short] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }

  "Bool" should "work with IntegerType" in {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        -100 to 100,
        Seq(Int.MinValue, Int.MaxValue)
      ),
      forceSparkType = IntegerType,
      convertToOriginalType = b => if (b.asInstanceOf[Int] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }

  "Bool" should "work with LongType" in {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        (-100 to 100) map (_.toLong),
        Seq(Long.MinValue, Long.MaxValue)
      ),
      forceSparkType = LongType,
      convertToOriginalType = b => if (b.asInstanceOf[Long] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }
}
