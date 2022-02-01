package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.ShortType
import org.scalatest.flatspec.AnyFlatSpec

class Int16Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type Int16" should "be written and read" in {
    testPrimitive("Int16", (1 to 100).map(_.toShort), row => row.getShort(0))
  }

  "Array(Int16)" should "be written and read" in {
    testArray("Int16", (1 to 100).map(_.toShort), ShortType)
  }
}
