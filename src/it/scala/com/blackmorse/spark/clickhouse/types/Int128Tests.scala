package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.DecimalType
import org.scalatest.flatspec.AnyFlatSpec

class Int128Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int128" should "be supported" in {
    testPrimitiveAndArray(
      typ = "Int128",
      seq = Seq((1 to 100) map (i => new java.math.BigDecimal(i))),
      rowConverter = row => row.getDecimal(0),
      sparkType = DecimalType(38, 0)
    )
  }
}
