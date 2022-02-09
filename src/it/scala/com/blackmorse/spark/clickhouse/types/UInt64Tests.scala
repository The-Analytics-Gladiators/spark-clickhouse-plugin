package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DecimalType, LongType}
import org.scalatest.flatspec.AnyFlatSpec

class UInt64Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Primitive type UInt64" should "be written and read" in {
    testPrimitive("UInt64", (1 to 100).map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
  }

//  "Big Primitives of UInt64" should "be written and read" in {
//    testPrimitive("UInt64", (100 to 100).map(i => new java.math.BigDecimal(s"10223372036854775$i")), row => row.getDecimal(0))
//  }

//  "Primitive type UInt64" should "be written and read" in {
//    testPrimitive("UInt64", Seq("10223372036854775807").map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
//  }

//  "Array(UInt64)" should "be written and read" in {
//    testArray("UInt64", (1 to 100).map(i => new java.math.BigDecimal(i)), DecimalType(38, 0))
//  }
}
