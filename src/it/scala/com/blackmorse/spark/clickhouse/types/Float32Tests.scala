package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.FloatType
import org.scalatest.flatspec.AnyFlatSpec

class Float32Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type Float32" should "be written and read" in {
    testPrimitive("Float32", (1 to 100).map(_.toFloat), row => row.getFloat(0))
  }

  "Array(Float32)" should "be written and read" in {
    testArray("Float32", (1 to 100).map(_.toFloat), FloatType)
  }
}
