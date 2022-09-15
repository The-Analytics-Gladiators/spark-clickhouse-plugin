package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt64
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class Int64Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int64" should "be supported" in {
    testPrimitiveAndArray(ClickhouseInt64(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toLong),
        Seq(Long.MinValue, Long.MaxValue)
      ),
      rowConverter = row => row.getLong(0)
    )
  }
}
