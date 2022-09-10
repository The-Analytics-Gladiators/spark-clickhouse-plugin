package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt128
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
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
}
