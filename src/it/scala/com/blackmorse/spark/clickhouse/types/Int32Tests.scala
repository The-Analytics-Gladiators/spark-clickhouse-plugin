package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.IntegerType
import org.scalatest.flatspec.AnyFlatSpec

class Int32Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type Int32" should "be written and read" in {
    testPrimitive("Int32", 1 to 100, row => row.getInt(0))
  }

  "Array(Int32)" should "be written and read" in {
    testArray("Int32", 1 to 100, IntegerType)
  }
}
