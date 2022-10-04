package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt16
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{ByteType, ShortType}
import org.scalatest.flatspec.AnyFlatSpec

class UInt16Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt16" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt16(false, lowCardinality = false))(
      cases = Seq(
        1 to 100,
        Seq(0, 65535)
      ),
      rowConverter = row => row.getInt(0)
    )
  }

  "ByteType" should "be supported by UInt16" in {
    testPrimitiveAndArray(ClickhouseUInt16(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq(0.toByte, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = _.asInstanceOf[Byte].toInt,
      rowConverter = row => row.getInt(0)
    )
  }

  "ShortType" should "be supported by UInt16" in {
    testPrimitiveAndArray(ClickhouseUInt16(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(0.toShort, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = _.asInstanceOf[Short].toInt,
      rowConverter = row => row.getInt(0)
    )
  }
}
