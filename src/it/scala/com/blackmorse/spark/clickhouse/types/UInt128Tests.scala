package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt128
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class UInt128Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt128" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt128(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toString),
        Seq("0", "340282366920938463463374607431768211455")
      ),
      rowConverter = row => row.getString(0)
    )
  }
}
