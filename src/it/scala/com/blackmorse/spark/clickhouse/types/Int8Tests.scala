package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.ByteType
import org.scalatest.flatspec.AnyFlatSpec

class Int8Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Int8" should "be supported" in {
    testPrimitiveAndArray(
      typ = "Int8",
      seq = Seq((1 to 100) map (_.toByte)),
      rowConverter = row => row.getByte(0),
      sparkType = ByteType
    )
  }
}
