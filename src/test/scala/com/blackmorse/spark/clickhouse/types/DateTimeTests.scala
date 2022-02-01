package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.TimestampType
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
  "Primitive type DateTime" should "be written and read" in {
    testPrimitive("DateTime('Europe/Moscow')", (1 to 100).map(time), row => row.getTimestamp(0))
  }

  "Primitive type DateTime64" should "be written and read" in {
    testPrimitive("DateTime64(8)", (1 to 100).map(time), row => row.getTimestamp(0))
  }

  "Array(DateTime)" should "be written and read" in {
    testArray("DateTime", (1 to 100).map(time), TimestampType)
  }

  //TODO
//  "Array(DateTime64)" should "be written and read" in {
//    testArray("DateTime64", (1 to 100).map(time), TimestampType)
//  }
}
