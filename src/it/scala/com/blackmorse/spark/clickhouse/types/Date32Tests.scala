package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseDate, ClickhouseDate32}
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.extra.Days

import java.sql.Date
import java.time.{LocalDate, ZoneId}

class Date32Tests extends AnyFlatSpec with DataFrameSuiteBase {
  private def date(offset: Int): Date =
    new Date(
      LocalDate.now().plus(Days.of(offset))
        .atStartOfDay(ZoneId.systemDefault())
        .toInstant
        .getEpochSecond * 1000
    )

  implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
  import sqlContext.implicits._

  "Date32" should "be supported" in {
    testPrimitiveAndArray(ClickhouseDate32(nullable = false, lowCardinality = false))(
      seq = Seq((1 to 100).map(date)),
      rowConverter = row => row.getDate(0),
    )
  }
}
