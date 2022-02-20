package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.StringType
import org.scalatest.flatspec.AnyFlatSpec

class StringTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "String" should "be supported" in {
    testPrimitiveAndArray(
      typ = "String",
      seq = Seq((1 to 100) map (_.toString)),
      rowConverter = row => row.getString(0),
      sparkType = StringType
    )
  }
}
