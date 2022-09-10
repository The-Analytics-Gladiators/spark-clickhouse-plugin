package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt8
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class Int8Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int8" should "be supported" in {
    testPrimitiveAndArray(clickhouseType = ClickhouseInt8(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toByte),
        Seq((-127).toByte, 128.toByte)
      ),
      rowConverter = row => row.getByte(0),
    )
  }
}
