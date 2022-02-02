package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.ByteType
import org.scalatest.flatspec.AnyFlatSpec

class Int8Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._
  "Primitive type Int8" should "be written and read" in {
    testPrimitive("Int8", (1 to 100).map(_.toByte), row => row.getByte(0))
  }

  "Array(Int8)" should "be written and read" in {
    testArray("Int8", (1 to 100).map(_.toByte), ByteType)
  }
}
