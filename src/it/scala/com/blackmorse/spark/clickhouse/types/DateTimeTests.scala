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

  implicit val timestampOrdering: Ordering[Timestamp] = Ordering.by(_.getTime)
  "DateTime64" should "be supported" in {
    (1 to 8) foreach (i =>
      testPrimitiveAndArray(ClickhouseDateTime64(i, nullable = false))(
        seq = Seq((1 to 100) map time),
        rowConverter = row => row.getTimestamp(0)
      ))
  }

  "DateTime" should "be supported" in {
    testPrimitiveAndArray(ClickhouseDateTime(nullable = false, lowCardinality = false))(
      seq = Seq((1 to 100) map time),
      rowConverter = row => row.getTimestamp(0)
    )
  }
}
