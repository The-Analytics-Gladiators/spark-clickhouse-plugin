package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt128
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class UInt128Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt128" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt128(nullable = false, lowCardinality = false))(
      seq = Seq((1 to 100) map (i => new java.math.BigDecimal(i))),
      rowConverter = row => row.getDecimal(0)
    )
  }
}
