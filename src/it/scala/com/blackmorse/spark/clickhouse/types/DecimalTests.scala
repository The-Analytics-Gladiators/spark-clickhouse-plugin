package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseDecimal
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.scalatest.flatspec.AnyFlatSpec

class DecimalTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  val comparator: (String, String) => Boolean =
    (r, e) => new java.math.BigDecimal(r).subtract(new java.math.BigDecimal(e)).abs().compareTo(new java.math.BigDecimal("0.01")) == -1

  "Decimal" should "be supported" in {
    testPrimitiveAndArray(ClickhouseDecimal(10, 6, nullable = false))(
      cases = Seq(
        (1 to 100) map (i => s"$i.$i"),
        Seq(s"${"9" * 10}.${"9" * 6}")
      ),
      rowConverter = row => row.getString(0),
      comparator = comparator
    )
  }

  "FloatType" should "be supported by Decimal" in {
    testPrimitiveAndArray(ClickhouseDecimal(38, 6, nullable = false))(
      cases = Seq(
        (1 to 100) map (_.toFloat),
        (1 to 100) map (f => f + f.toFloat / 100)
      ),
      forceSparkType = FloatType,
      convertToOriginalType = _.asInstanceOf[Float].toString,
      rowConverter = row => row.getString(0),
      comparator = comparator
    )
  }

  "DoubleType" should "be supported by Decimal" in {
    testPrimitiveAndArray(ClickhouseDecimal(38, 6, nullable = false))(
      cases = Seq(
        (1 to 100) map (_.toDouble),
        (1 to 100) map (f => f + f.toDouble / 100)
      ),
      forceSparkType = DoubleType,
      convertToOriginalType = _.asInstanceOf[Double].toString,
      rowConverter = row => row.getString(0),
      comparator = comparator
    )
  }
}
