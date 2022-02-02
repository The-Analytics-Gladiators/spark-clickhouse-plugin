package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DecimalType, StringType}
import org.scalatest.flatspec.AnyFlatSpec

class StringTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type String" should "be written and read" in {
    testPrimitive("String", (1 to 100).map(_.toString), row => row.getString(0))
  }

  "Array(String)" should "be written and read" in {
    testArray("String", (1 to 100).map(_.toString), StringType)
  }
}
