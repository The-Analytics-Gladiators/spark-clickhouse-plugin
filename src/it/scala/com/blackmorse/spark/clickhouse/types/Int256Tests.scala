package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseInt256
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class Int256Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int256" should "be supported" in {
    testPrimitiveAndArray(clickhouseType = ClickhouseInt256(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toString),
        Seq("-57896044618658097711785492504343953926634992332820282019728792003956564819968",
          "57896044618658097711785492504343953926634992332820282019728792003956564819967")
      ),
      rowConverter = row => row.getString(0),
    )
  }
}
