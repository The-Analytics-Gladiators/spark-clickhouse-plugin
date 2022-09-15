package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt32
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class UInt32Tests  extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt32" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt32(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (_.toLong),
        Seq(0, 4294967295L)
      ),
      rowConverter = row => row.getLong(0)
    )
  }
}
