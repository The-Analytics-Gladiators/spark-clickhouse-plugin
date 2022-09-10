package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt16
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class Int16Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int16" should "be supported" in {
    testPrimitiveAndArray(ClickhouseInt16(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toShort),
        Seq(Short.MinValue, Short.MaxValue)
      ),
      rowConverter = row => row.getShort(0)
    )
  }
}
