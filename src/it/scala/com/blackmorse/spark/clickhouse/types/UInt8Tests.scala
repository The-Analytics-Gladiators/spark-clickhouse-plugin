package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.ShortType
import org.scalatest.flatspec.AnyFlatSpec

class UInt8Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "UInt8" should "be supported" in {
    testPrimitiveAndArray(
      typ = "UInt8",
      seq = Seq((1 to 100) map (_.toShort)),
      rowConverter = row => row.getShort(0),
      sparkType = ShortType
    )
  }
}
