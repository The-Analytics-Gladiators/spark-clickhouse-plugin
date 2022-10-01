package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt8
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.ByteType
import org.scalatest.flatspec.AnyFlatSpec

class UInt8Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt8" should "be supported" in {
    testPrimitiveAndArray(clickhouseType = ClickhouseUInt8(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(0.toShort, 255.toShort)
      ),
      rowConverter = row => row.getShort(0),
    )
  }

  "ByteType" should "be supported by UInt8" in {
    testPrimitiveAndArray(ClickhouseUInt8(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq(0.toByte, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = _.asInstanceOf[Byte].toShort,
      rowConverter = row => row.getShort(0)
    )
  }
}
