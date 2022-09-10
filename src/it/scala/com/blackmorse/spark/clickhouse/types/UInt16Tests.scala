package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt16
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class UInt16Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt16" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt16(false, lowCardinality = false))(
      cases = Seq(
        1 to 100,
        Seq(0, 65535)
      ),
      rowConverter = row => row.getInt(0)
    )
  }
}
