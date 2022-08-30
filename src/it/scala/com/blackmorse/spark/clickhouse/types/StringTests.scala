package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseString
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class StringTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "String" should "be supported" in {
    testPrimitiveAndArray(ClickhouseString(nullable = false, lowCardinality = false))(
      seq = Seq((1 to 100) map (_.toString)),
      rowConverter = row => row.getString(0)
    )
  }
}
