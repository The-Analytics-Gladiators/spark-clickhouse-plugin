package com.blackmorse.spark.clickhouse.types

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import org.apache.spark.sql.types.ShortType

class UInt8Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type UInt8" should "be written and read" in {
    testPrimitive("UInt8", (1 to 100).map(_.toShort), row => row.getShort(0))
  }

  "Array(UInt8)" should "be written and read" in {
    testArray("UInt8", (1 to 100).map(_.toShort), ShortType)
  }
}
