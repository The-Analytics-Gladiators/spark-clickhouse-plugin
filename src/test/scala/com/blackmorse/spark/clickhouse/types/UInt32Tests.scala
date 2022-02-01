package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.LongType
import org.scalatest.flatspec.AnyFlatSpec

class UInt32Tests  extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type UInt32" should "be written and read" in {
    testPrimitive("UInt32", (1 to 100).map(_.toLong), row => row.getLong(0))
  }

  "Array(UInt32)" should "be written and read" in {
    testArray("UInt32", (1 to 100).map(_.toLong), LongType)
  }
}
