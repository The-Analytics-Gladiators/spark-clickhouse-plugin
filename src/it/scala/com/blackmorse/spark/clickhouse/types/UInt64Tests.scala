package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseUInt64
import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive, testPrimitiveAndArray}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

class UInt64Tests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  //This PR should resolve https://github.com/ClickHouse/clickhouse-jdbc/pull/1040
  "Big Primitives of UInt64" should "be written and read" ignore {
    testPrimitive(ClickhouseUInt64(nullable = false, lowCardinality = false))(
      (1 to 100).map(i => new java.math.BigDecimal(s"10223372036854775$i")),
      row => row.getDecimal(0))
  }

  ignore should "Ignore due to clickhouse-jdbc upstream but" in {
    testArray(ClickhouseUInt64(nullable = false, lowCardinality = false))(
      (1 to 100).map(i => new java.math.BigDecimal(s"10223372036854775$i"))
    )
  }

  "UInt64" should "be supported" in {
    testPrimitiveAndArray(ClickhouseUInt64(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map (i => new java.math.BigDecimal(i)),
//        Seq(new java.math.BigDecimal(0), new java.math.BigDecimal("18446744073709551615"))// Again after that PR
      ),
      rowConverter = row => row.getDecimal(0)
    )
  }
}
