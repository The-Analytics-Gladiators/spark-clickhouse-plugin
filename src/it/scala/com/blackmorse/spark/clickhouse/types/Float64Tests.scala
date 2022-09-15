package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseFloat64
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class Float64Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Float64" should "be supported" in {
    testPrimitiveAndArray(ClickhouseFloat64(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toDouble),
        (1 to 100) map (f => f + f.toDouble / 100),
        Seq(Double.MinValue, Double.MaxValue)
      ),
      rowConverter = row => row.getDouble(0)
    )
  }
}
