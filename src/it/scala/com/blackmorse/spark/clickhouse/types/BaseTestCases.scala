package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.ClickhouseTests.{url, withTable}
import com.blackmorse.spark.clickhouse.sql.types.{ClickhousePrimitive, ClickhouseType}
import com.holdenkarau.spark.testing.SparkSessionProvider.sqlContext
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Row, SQLContext}
import org.scalatest.matchers.should

import scala.reflect.ClassTag


object BaseTestCases extends should.Matchers {
  import com.blackmorse.spark.clickhouse._
  import collection.JavaConverters._
  val host = "localhost"
  val port = 8123
  val table = "default.test_table"

  def testPrimitiveAndArray(clickhouseType: ClickhouseType)(seq: Seq[Seq[clickhouseType.T]],
                                                            //TODO should be in ClickhouseType ?
                                                            rowConverter: Row => clickhouseType.T,
                                                            comparator: (clickhouseType.T, clickhouseType.T) => Boolean = (t: clickhouseType.T, s: clickhouseType.T) => t == s)
                           (implicit ord: Ordering[clickhouseType.T], encoder: Encoder[Seq[clickhouseType.T]], ct: ClassTag[clickhouseType.T], sqlContext: SQLContext): Unit = {
    seq.foreach(seqInner =>
      testPrimitive(clickhouseType)(seqInner, rowConverter, comparator))
    testPrimitive(clickhouseType)(seq.flatten ++ Seq(null), rowConverter, comparator)
    testPrimitive(clickhouseType)(Seq(null), rowConverter, comparator)

    testArray(clickhouseType)(seq.flatten, comparator)
  }

  def testArray(clickhouseType: ClickhouseType)(seq: Seq[clickhouseType.T],
                                                comparator: (clickhouseType.T, clickhouseType.T) => Boolean = (t: clickhouseType.T, s: clickhouseType.T) => t == s)
               (implicit ord: Ordering[clickhouseType.T], encoder: Encoder[Seq[clickhouseType.T]], ct: ClassTag[clickhouseType.T], sqlContext: SQLContext): Unit = {
    val clickhouseTypeName = clickhouseType.clickhouseDataTypeString
    val sparkType = clickhouseType.toSparkType()
    val typeDefaultValue = clickhouseType.defaultValue

    val sc = sqlContext.sparkContext
    withTable(Seq(s"a Array($clickhouseTypeName)"), "a") {
      val elements = Seq(
        seq ++ Seq(null),
        seq,
        Seq(null),
        Seq(),
        null
      )

      val schema = StructType(Seq(StructField("a", ArrayType(sparkType), true)))

      val rows = elements.map(el => Row.fromSeq(Seq(el)))
      sqlContext.createDataFrame(sc.parallelize(rows), schema)
        .write.clickhouse(host, port, table)

      val dataFrame = sqlContext.read.clickhouse(host, port, table)

      dataFrame.schema.length should be(1)
      dataFrame.schema.head.dataType should be(ArrayType(sparkType, false))

      val result: Array[java.util.List[clickhouseType.T]] = dataFrame
        .rdd
        .map(row => row.getList[clickhouseType.T](0))
        .collect()
        .sortBy(_.size())

      val expected: Seq[Seq[clickhouseType.T]] = elements
        .map {
          case null => Seq[clickhouseType.T]()
          case arr: Seq[clickhouseType.T] => arr map {
            case null => typeDefaultValue
            case t: clickhouseType.T => t
          }
        }
      .sortBy(_.size)

      result.length should be (expected.size)

      result.zip(expected).foreach { case (l, r) =>
        l.size() should be (r.size)
        l.asScala.zip(r).foreach { case (fromCh, original) =>
          assert(comparator(fromCh, original), s"Expected values: $original. Actual value: $fromCh")
        }
      }
    }
  }

  def testPrimitive(clickhouseType: ClickhouseType)(seq: Seq[Any],
                                                    rowConverter: Row => clickhouseType.T,
                                                    comparator: (clickhouseType.T, clickhouseType.T) => Boolean = (t: clickhouseType.T, s: clickhouseType.T) => t == s)
                   (implicit ord: Ordering[clickhouseType.T], ct: ClassTag[clickhouseType.T], sqlContext: SQLContext): Unit = {

    val clickhouseTypeName = clickhouseType.clickhouseDataTypeString

    val sc = sqlContext.sparkContext
    withTable(Seq(s"a $clickhouseTypeName"), "a") {
      val rows = seq.map(el => Row.fromSeq(Seq(el)))
      val schema = StructType(Seq(StructField("a", clickhouseType.toSparkType(), nullable = true)))

        sqlContext.createDataFrame(sc.parallelize(rows),  schema)
          .write.clickhouse(host, port, table)

      val res = sqlContext.read.clickhouse(host, port, table).rdd
        .map(row => rowConverter(row)).collect()

      val expected = seq.map{
        case null => clickhouseType.defaultValue
        case t: clickhouseType.T => t
      }.sorted

      res.sorted.zip(expected).foreach { case (result, expected) =>
        assert(comparator(result, expected), s"Expected values: $expected. Actual value: $result")
      }
    }
  }
}
