package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.apache.spark.sql.{Encoder, Row, SQLContext}
import org.scalatest.matchers.should

import scala.reflect.ClassTag

object BaseTestCases extends should.Matchers {
  import com.blackmorse.spark.clickhouse._
  import collection.JavaConverters._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  def testPrimitiveAndArray[T: Encoder](typ: String,
                                        seq: Seq[Seq[T]],
                                        rowConverter: Row => T,
                                        sparkType: DataType,
                                        comparator: (T, T) => Boolean = (t: T, s: T) => t == s)
                                       (implicit ord: Ordering[T], encoder: Encoder[Seq[T]], ct: ClassTag[T], sqlContext: SQLContext): Unit = {
    seq.foreach(seqInner =>
      testPrimitive(typ, seqInner, rowConverter, comparator))
    testPrimitive(typ, seq.flatten ++ Seq(), rowConverter, comparator)

    testArray(typ, seq.flatten , sparkType, comparator)
  }

  def testPrimitive[T : Encoder](typ: String, seq: Seq[T], rowConverter: Row => T,
                                 comparator: (T, T) => Boolean = (t: T, s: T) => t == s)
                                (implicit ord: Ordering[T], ct: ClassTag[T], sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val sc = sqlContext.sparkContext
    withTable(Seq(s"a $typ"), "a") {
      sc.parallelize(seq).toDF("a")
        .write.clickhouse(host, port, table)

      val res = sqlContext.read.clickhouse(host, port, table).rdd
        .map(row => rowConverter(row)).collect()

      res.sorted.zip(seq.sorted).foreach{ case (result, expected) =>
        assert(comparator(result, expected), s"Expected values: $expected. Actual value: $result")
      }
    }
  }

  def testArray[T: Encoder](typ: String, seq: Seq[T], sparkType: DataType,
                            comparator: (T, T) => Boolean = (t: T, s: T) => t == s)
    (implicit ord: Ordering[T], encoder: Encoder[Seq[T]], ct: ClassTag[T], sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    val sc = sqlContext.sparkContext
    withTable(Seq(s"a Array($typ)"), "a") {
      val input = Seq(seq, Seq())
      sc.parallelize(input)
        .toDF("a")
        .write.clickhouse(host, port, table)

      val dataFrame = sqlContext.read.clickhouse(host, port, table)

      dataFrame.schema.length should be (1)
      dataFrame.schema.head.dataType should be (ArrayType(sparkType, false))

      val res = dataFrame
        .rdd
        .map(row => row.getList[T](0))
        .collect()

      res.sortBy(_.size()).zip(input.sortBy(_.size)).foreach { case (l, r) =>
        l.asScala.zip(r).foreach{ case (fromCh, original) =>
          assert(comparator(fromCh, original), s"Expected values: $original. Actual value: $fromCh")
        }
      }
    }
  }
}
