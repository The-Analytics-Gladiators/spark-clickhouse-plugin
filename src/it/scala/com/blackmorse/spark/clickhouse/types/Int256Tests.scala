package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt256
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class Int256Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int256" should "be supported" in {
    testPrimitiveAndArray(clickhouseType = ClickhouseInt256(nullable = false, lowCardinality = false))(
      seq = Seq((1 to 100) map (i => new java.math.BigDecimal(i)))
      ,
      rowConverter = row => row.getDecimal(0),
    )
  }
}
