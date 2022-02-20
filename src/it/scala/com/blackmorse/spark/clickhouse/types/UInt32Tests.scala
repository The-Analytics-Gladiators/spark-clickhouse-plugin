package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.LongType
import org.scalatest.flatspec.AnyFlatSpec

class UInt32Tests  extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt32" should "be supported" in {
    testPrimitiveAndArray(
      typ = "UInt32",
      seq = Seq((1 to 100) map (_.toLong)),
      rowConverter = row => row.getLong(0),
      sparkType = LongType
    )
  }
}
