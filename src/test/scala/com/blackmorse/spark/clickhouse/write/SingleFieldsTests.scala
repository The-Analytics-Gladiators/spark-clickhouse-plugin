package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Encoder, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.threeten.extra.Days

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, ZoneId, ZonedDateTime}
import scala.reflect.ClassTag

class SingleFieldsTests extends AnyFlatSpec with DataFrameSuiteBase {
  import com.blackmorse.spark.clickhouse._
  import sqlContext.implicits._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  def testPrimitive[T : Encoder](typ: String, seq: Seq[T], rowConverter: Row => T)(implicit ord: Ordering[T], ct: ClassTag[T]) = {
    withTable(Seq(s"a $typ"), "a") {
      sc.parallelize(seq).toDF("a")
        .write.clickhouse(host, port, table)

      val res = sqlContext.read.clickhouse(host, port, table).rdd
        .map(row => rowConverter(row)).collect()

      assert(res.sorted sameElements seq.sorted)
    }
  }

  "Primitive type Int8" should "be written and read" in {
    testPrimitive("Int8", (1 to 100).map(_.toByte), row => row.getByte(0))
  }

  "Primitive type UInt8" should "be written and read" in {
    testPrimitive("UInt8", (1 to 100).map(_.toShort), row => row.getShort(0))
  }

  "Primitive type Int16" should "be written and read" in {
    testPrimitive("Int16", (1 to 100).map(_.toShort), row => row.getShort(0))
  }

  "Primitive type UInt16" should "be written and read" in {
    testPrimitive("UInt16", 1 to 100, row => row.getInt(0))
  }

  "Primitive type Int32" should "be written and read" in {
    testPrimitive("Int32", 1 to 100, row => row.getInt(0))
  }

  "Primitive type UInt32" should "be written and read" in {
    testPrimitive("UInt32", (1 to 100).map(_.toLong), row => row.getLong(0))
  }

  "Primitive type Int64" should "be written and read" in {
    testPrimitive("Int64", (1 to 100).map(_.toLong), row => row.getLong(0))
  }

  "Primitive type UInt64" should "be written and read" in {
    testPrimitive("UInt64", (1 to 100).map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
  }

  "Primitive type Int128" should "be written and read" in {
    testPrimitive("Int128", (1 to 100).map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
  }

  "Primitive type UInt128" should "be written and read" in {
    testPrimitive("UInt128", (1 to 100).map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
  }

  "Primitive type Int256" should "be written and read" in {
    testPrimitive("Int256", (1 to 100).map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
  }

  "Primitive type UInt256" should "be written and read" in {
    testPrimitive("UInt256", (1 to 100).map(i => new java.math.BigDecimal(i)), row => row.getDecimal(0))
  }

  "Primitive type String" should "be written and read" in {
    testPrimitive("String", (1 to 100).map(_.toString), row => row.getString(0))
  }

  "Primitive type Float32" should "be written and read" in {
    testPrimitive("Float32", (1 to 100).map(_.toFloat), row => row.getFloat(0))
  }

  "Primitive type Float64" should "be written and read" in {
    testPrimitive("Float64", (1 to 100).map(_.toDouble), row => row.getDouble(0))
  }


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

  //For more recent Clickhouses:
//  "Primitive type Date32" should "be written and read" in {
//    testPrimitive("Date32", (1 to 100).map(date), row => row.getDate(0))
//  }

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

  "Primitive type Decimal" should "be written and read" in {
    withTable(Seq(s"a Decimal(8,4)"), "a") {
      val seq = (1 to 100) map (i => new java.math.BigDecimal(s"$i.$i"))
      sc.parallelize(seq).toDF("a")
        .write.clickhouse(host, port, table)

      val res = sqlContext.read.clickhouse(host, port, table).rdd
        .map(_.getDecimal(0)).collect()

      res.sorted.zip(seq.sorted).foreach{case (result, expected) =>
        result.compareTo(expected)
      }
    }
  }

}
