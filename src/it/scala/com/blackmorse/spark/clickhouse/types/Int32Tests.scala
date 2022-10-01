package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt32
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{ByteType, ShortType}
import org.scalatest.flatspec.AnyFlatSpec

class Int32Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int32" should "be supported" in {
    testPrimitiveAndArray(ClickhouseInt32(nullable = false, lowCardinality = false))(
      cases  = Seq(
        1 to 100,
        Seq(Int.MinValue, Int.MaxValue)
      ),
      rowConverter = row => row.getInt(0))
  }

  "ByteType" should "work with Int32" in {
    testPrimitiveAndArray(ClickhouseInt32(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq(Byte.MinValue, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = _.asInstanceOf[Byte].toInt,
      rowConverter = _.getInt(0)
    )
  }

  "ShortType" should "work with Int32" in {
    testPrimitiveAndArray(ClickhouseInt32(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(Short.MinValue, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = _.asInstanceOf[Short].toInt,
      rowConverter = _.getInt(0)
    )
  }
}
