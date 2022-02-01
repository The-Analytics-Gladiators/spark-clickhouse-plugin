package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.DecimalType
import org.scalatest.flatspec.AnyFlatSpec

class UInt128Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type UInt128" should "be written and read" in {
    testPrimitive("UInt128", (1 to 100).map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
  }

  "Array(UInt128)" should "be written and read" in {
    testArray("UInt128", (1 to 100).map(i => new java.math.BigDecimal(i)), DecimalType(38, 0))
  }
}
