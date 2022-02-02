package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.IntegerType
import org.scalatest.flatspec.AnyFlatSpec

class UInt16Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type UInt16" should "be written and read" in {
    testPrimitive("UInt16", 1 to 100, row => row.getInt(0))
  }

  "Array(UInt16)" should "be written and read" in {
    testArray("UInt16", 1 to 100, IntegerType)
  }
}
