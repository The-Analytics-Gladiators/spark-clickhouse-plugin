package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DateType, FloatType}
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.extra.Days

import java.sql.Date
import java.time.{LocalDate, ZoneId}

class DateTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  def date(offset: Int): Date =
    new Date(
      LocalDate.now().plus(Days.of(offset))
        .atStartOfDay(ZoneId.systemDefault())
        .toInstant
        .getEpochSecond * 1000
    )

  implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
  "Primitive type Date" should "be written and read" in {
    testPrimitive("Date", (1 to 100).map(date), row => row.getDate(0))
  }

  //TODO can't insert days starting from 02.02.2022
//  "Array(Date)" should "be written and read" in {
//    testArray("Date", (1 to 100).map(date), DateType)
//  }
}
