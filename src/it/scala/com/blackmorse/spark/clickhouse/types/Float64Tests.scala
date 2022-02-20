package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.DoubleType
import org.scalatest.flatspec.AnyFlatSpec

class Float64Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Float64" should "be supported" in {
    testPrimitiveAndArray(
      typ = "Float64",
      seq = Seq((1 to 100) map (_.toDouble)),
      rowConverter = row => row.getDouble(0),
      sparkType = DoubleType
    )
  }
}
