package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testArray, testPrimitive, testPrimitiveAndArray}
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
  "Primitive type DateTime64" should "be written and read" in {
    testPrimitive("DateTime64(8)", (1 to 100).map(time), row => row.getTimestamp(0))
  }

  "DateTime" should "be supported" in {
    testPrimitiveAndArray(
      typ = "DateTime",
      seq = Seq((1 to 100) map time),
      rowConverter = row => row.getTimestamp(0),
      sparkType = TimestampType
    )
  }

  //TODO testPrimitiveAndArray for DateTime32
//  "Array(DateTime64)" should "be written and read" in {
//    testArray("DateTime64", (1 to 100).map(time), TimestampType)
//  }
}
