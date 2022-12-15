package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseDateTime
import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseDate
import com.blackmorse.spark.clickhouse.types.BaseTestCases.testPrimitiveAndArray
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}


class DatesBackwardCompatibilityTest extends AnyFlatSpec with DataFrameSuiteBase {

  import sqlContext.implicits._
  implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
  implicit val timestampOrdering: Ordering[Timestamp] = Ordering.by(_.getTime)

  private lazy val nowTimestamp: Long = System.currentTimeMillis()

  "Date" should "write in ch DateTime column" in {
    testPrimitiveAndArray(ClickhouseDateTime(nullable = false, lowCardinality = false))(
      cases = Seq(Seq(new Date(nowTimestamp))),
      rowConverter = row => row.getTimestamp(0),
      forceSparkType = DateType,
      convertToOriginalType = t => Timestamp.valueOf(t.toString + " 00:00:00.0")
    )
  }

  "DateTime" should "write in ch date column" in {
    testPrimitiveAndArray(ClickhouseDate(nullable = false, lowCardinality = false))(
      cases = Seq(Seq(new Timestamp(nowTimestamp))),
      rowConverter = row => row.getDate(0),
      forceSparkType = TimestampType,
      convertToOriginalType = _ => Date.valueOf(LocalDate.now())
    )
  }

}
