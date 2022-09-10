package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseDate, ClickhouseDate32}
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.extra.Days

import java.sql.Date
import java.time.{LocalDate, ZoneId}

class Date32Tests extends AnyFlatSpec with DataFrameSuiteBase {
  private def localDateToDate(localDate: LocalDate): Date =
    new Date(localDate.atStartOfDay(ZoneId.systemDefault())
      .toInstant
      .getEpochSecond * 1000)

  private def date(offset: Int): Date =
    localDateToDate(
      LocalDate.now().plus(Days.of(offset))
    )

//  private val minDate = localDateToDate(LocalDate.of(1925, 1, 1)) //TODO Check with string type
  private val maxDate = localDateToDate(LocalDate.of(2283, 11, 11))

  implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
  import sqlContext.implicits._

  "Date32" should "be supported" in {
    testPrimitiveAndArray(ClickhouseDate32(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100).map(date),
        Seq(maxDate)
      ),
      rowConverter = row => row.getDate(0),
    )
  }
}
