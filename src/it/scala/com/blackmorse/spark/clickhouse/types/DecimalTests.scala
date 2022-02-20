package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.DecimalType
import org.scalatest.flatspec.AnyFlatSpec

class DecimalTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  val comparator: (java.math.BigDecimal, java.math.BigDecimal) => Boolean =
    (r, e) => r.subtract(e).abs().compareTo(new java.math.BigDecimal("0.01")) == -1

  "Decimal" should "be supported" in {
    testPrimitiveAndArray(
      typ = "Decimal(10, 6)",
      seq = Seq((1 to 100) map (i => new java.math.BigDecimal(s"$i.$i"))),
      rowConverter = row => row.getDecimal(0),
      sparkType = DecimalType(10, 6),
      comparator = comparator
    )
  }
}
