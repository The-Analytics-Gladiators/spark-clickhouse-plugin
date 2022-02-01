package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.scalatest.flatspec.AnyFlatSpec

class Float64Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type Float64" should "be written and read" in {
    testPrimitive("Float64", (1 to 100).map(_.toDouble), row => row.getDouble(0))
  }

  "Array(Float64)" should "be written and read" in {
    testArray("Float64", (1 to 100).map(_.toDouble), DoubleType)
  }
}
