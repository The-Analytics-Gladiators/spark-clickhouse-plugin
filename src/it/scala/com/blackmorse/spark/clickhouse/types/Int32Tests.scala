package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt32
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class Int32Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int32" should "be supported" in {
    testPrimitiveAndArray(ClickhouseInt32(nullable = false, lowCardinality = false))(
      seq  = Seq(1 to 100),
      rowConverter = row => row.getInt(0))
  }
}
