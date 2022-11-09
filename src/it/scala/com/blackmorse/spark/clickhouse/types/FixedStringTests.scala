package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseFixedString
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class FixedStringTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  "FixedString(3)" should "be supported" in {
    testPrimitiveAndArray(ClickhouseFixedString(nullable = false, lowCardinality = false, length = 3))(
      cases = Seq(
        (111 to 222) map (_.toString)
      ),
      rowConverter = row => row.getString(0)
    )
  }

  "FixedString(1)" should "be supported" in {
    testPrimitiveAndArray(ClickhouseFixedString(nullable = false, lowCardinality = false, length = 1))(
      cases = Seq(
        (1 to 9) map (_.toString)
      ),
      rowConverter = row => row.getString(0)
    )
  }

  "FixedString(16)" should "be supported" in {
    testPrimitiveAndArray(ClickhouseFixedString(nullable = false, lowCardinality = false, length = 36))(
      cases = Seq(
        List.fill(100)(java.util.UUID.randomUUID.toString)
      ),
      rowConverter = row => row.getString(0)
    )
  }
}
