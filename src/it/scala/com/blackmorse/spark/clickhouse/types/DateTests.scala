package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.DateType
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.extra.Days

import java.sql.Date
import java.time.{LocalDate, ZoneId}

class DateTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  private def date(offset: Int): Date =
    new Date(
      LocalDate.now().plus(Days.of(offset))
        .atStartOfDay(ZoneId.systemDefault())
        .toInstant
        .getEpochSecond * 1000
    )

  implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
  "Date" should "be supported" in {
    testPrimitiveAndArray(
      typ = "Date",
      seq = Seq((1 to 100).map(date)),
      rowConverter = row => row.getDate(0),
      sparkType = DateType
    )
  }
}
