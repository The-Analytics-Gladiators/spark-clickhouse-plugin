package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt256
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class UInt256Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt256" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt256(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toString),
        Seq("0", "115792089237316195423570985008687907853269984665640564039457584007913129639935")
      ),
      rowConverter = row => row.getString(0)
    )
  }
}
