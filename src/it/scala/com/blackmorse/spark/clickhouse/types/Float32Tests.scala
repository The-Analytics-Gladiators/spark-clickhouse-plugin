package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.FloatType
import org.scalatest.flatspec.AnyFlatSpec

class Float32Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "Float32" should "be supported" in {
    testPrimitiveAndArray(
      typ = "Float32",
      seq = Seq((1 to 100) map (_.toFloat)),
      rowConverter = row => row.getFloat(0),
      sparkType = FloatType
    )
  }
}
