package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseDecimal
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class DecimalTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  val comparator: (String, String) => Boolean =
    (r, e) => new java.math.BigDecimal(r).subtract(new java.math.BigDecimal(e)).abs().compareTo(new java.math.BigDecimal("0.01")) == -1

  "Decimal" should "be supported" in {
    testPrimitiveAndArray(ClickhouseDecimal(10, 6, nullable = false))(
      cases = Seq(
        (1 to 100) map (i => s"$i.$i")
      ),
      rowConverter = row => row.getString(0),
      comparator = comparator
    )
  }
}
