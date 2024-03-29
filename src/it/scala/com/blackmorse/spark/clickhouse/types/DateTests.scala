package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.DateTimeUtils.{date, localDateToDate, time}
import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseDate
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.TimestampType
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{Date, Timestamp}
import java.time.LocalDate

class DateTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  private val minDate = localDateToDate(LocalDate.of(1970, 1, 1))
  private val maxDate = localDateToDate(LocalDate.of(2149, 6, 6))

  implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
  "Date" should "be supported" in {
    testPrimitiveAndArray(ClickhouseDate(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100).map(date),
        Seq(minDate, maxDate)
      ),
      rowConverter = row => row.getDate(0),
    )
  }

  "DateTime" should "write in ch date column" in {
    testPrimitiveAndArray(ClickhouseDate(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map time
      ),
      rowConverter = row => row.getDate(0),
      forceSparkType = TimestampType,
      convertToOriginalType = timestamp => Date.valueOf(timestamp.asInstanceOf[Timestamp].toLocalDateTime.toLocalDate)
    )
  }
}
