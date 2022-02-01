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




  //For more recent Clickhouses:
//  "Primitive type Date32" should "be written and read" in {
//    testPrimitive("Date32", (1 to 100).map(date), row => row.getDate(0))
//  }



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
