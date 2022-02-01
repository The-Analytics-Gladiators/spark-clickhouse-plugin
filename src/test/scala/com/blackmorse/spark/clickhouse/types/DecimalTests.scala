package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DecimalType, DoubleType}
import org.scalatest.flatspec.AnyFlatSpec

class DecimalTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  val comparator: (java.math.BigDecimal, java.math.BigDecimal) => Boolean =
    (r, e) => r.subtract(e).abs().compareTo(new java.math.BigDecimal("0.01")) == -1

  "Primitive type Decimal" should "be written and read" in {
    testPrimitive("Decimal(8, 4)", (1 to 100).map(i => new java.math.BigDecimal(s"$i.$i")), row => row.getDecimal(0),
      comparator = comparator)
  }

  "Array(Decimal)" should "be written and read" in {
    testArray("Decimal(10, 6)", (1 to 100).map(i => new java.math.BigDecimal(s"$i.$i")), DecimalType(10, 6),
      comparator = comparator)
  }
}
