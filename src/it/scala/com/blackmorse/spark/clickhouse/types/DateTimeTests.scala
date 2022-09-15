package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.{ClickhouseDateTime, ClickhouseDateTime64}
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

class DateTimeTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._

  def time(offset: Int): Timestamp =
    new Timestamp(
      ZonedDateTime.now(ZoneId.of("UTC"))
        .plusHours(offset)
        .toInstant
        .getEpochSecond * 1000
    )

  //TODO fix UTC and system default zones to Clickhouse zone?
  private val minTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"))
    .toInstant.toEpochMilli

  private val maxTime = ZonedDateTime.of(2106, 2, 7, 6, 28, 15, 0, ZoneId.systemDefault())
    .toInstant.toEpochMilli

// TODO private val minTime64 = check 1900-01-01 with string
  private val maxTime64 = ZonedDateTime.of(2283, 11, 11, 23, 59, 59, 0, ZoneId.systemDefault())
    .toInstant.toEpochMilli

  implicit val timestampOrdering: Ordering[Timestamp] = Ordering.by(_.getTime)
  "DateTime64" should "be supported" in {
    (1 to 8) foreach (i =>
      testPrimitiveAndArray(ClickhouseDateTime64(i, nullable = false))(
        cases = Seq(
          (1 to 100) map time,
          Seq(new Timestamp(minTime), new Timestamp(maxTime64))
        ),
        rowConverter = row => row.getTimestamp(0)
      ))
  }

  "DateTime" should "be supported" in {
    testPrimitiveAndArray(ClickhouseDateTime(nullable = false, lowCardinality = false))(
      cases = Seq(
        (1 to 100) map time,
        Seq(new Timestamp(minTime), new Timestamp(maxTime))
      ),
      rowConverter = row => row.getTimestamp(0)
    )
  }
}
