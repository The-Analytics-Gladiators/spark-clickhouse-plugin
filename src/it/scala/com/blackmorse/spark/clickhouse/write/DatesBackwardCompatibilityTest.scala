package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.DateTimeUtils.{date, time}
import com.blackmorse.spark.clickhouse.sql.types.ClickhouseDateTime
import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseDate
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{Date, Timestamp}
import java.time.LocalDate
import java.time.temporal.ChronoUnit


class DatesBackwardCompatibilityTest extends AnyFlatSpec with DataFrameSuiteBase {

  import sqlContext.implicits._
  implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
  implicit val timestampOrdering: Ordering[Timestamp] = Ordering.by(_.getTime)

  "Date" should "write in ch DateTime column" in {
    testPrimitiveAndArray(ClickhouseDateTime(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100).map(date)
      ),
      rowConverter = row => row.getTimestamp(0),
      forceSparkType = DateType,
      convertToOriginalType = date => Timestamp.valueOf(LocalDate.parse(date.toString).atStartOfDay()
        .truncatedTo(ChronoUnit.MILLIS))
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
