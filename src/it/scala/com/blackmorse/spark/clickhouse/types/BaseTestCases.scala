package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.blackmorse.spark.clickhouse.sql.types.ClickhouseType
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

  def testPrimitiveAndArray(clickhouseType: ClickhouseType)(cases: Seq[Seq[Any]],
                                                            //TODO should be in ClickhouseType ?
                                                            rowConverter: Row => clickhouseType.T,
                                                            forceSparkType: DataType = clickhouseType.toSparkType(),
                                                            convertToOriginalType: Any => clickhouseType.T = t => t.asInstanceOf[clickhouseType.T],
                                                            comparator: (clickhouseType.T, clickhouseType.T) => Boolean = (t: clickhouseType.T, s: clickhouseType.T) => t == s)
                           (implicit ord: Ordering[clickhouseType.T], encoder: Encoder[Seq[clickhouseType.T]], ct: ClassTag[clickhouseType.T], sqlContext: SQLContext): Unit = {
    cases.foreach(seq => {
      testPrimitive(clickhouseType)(seq, rowConverter, forceSparkType, convertToOriginalType, comparator)
      testPrimitive(clickhouseType)(seq ++ Seq(null), rowConverter, forceSparkType, convertToOriginalType, comparator)
      testPrimitive(clickhouseType)(Seq(null), rowConverter, forceSparkType, convertToOriginalType, comparator)

      testArray(clickhouseType)(seq, forceSparkType, convertToOriginalType, comparator)
    })
  }

  def testArray(clickhouseType: ClickhouseType)(seq: Seq[Any],
                                                sparkType: DataType,
                                                convertToOriginalType: Any => clickhouseType.T,
                                                comparator: (clickhouseType.T, clickhouseType.T) => Boolean = (t: clickhouseType.T, s: clickhouseType.T) => t == s)
               (implicit ord: Ordering[clickhouseType.T], encoder: Encoder[Seq[clickhouseType.T]], ct: ClassTag[clickhouseType.T], sqlContext: SQLContext): Unit = {
    val clickhouseTypeName = clickhouseType.clickhouseDataTypeString
    val typeDefaultValue = clickhouseType.defaultValue

    val sc = sqlContext.sparkContext
    val elements = Seq(
      seq ++ Seq(null),
      seq,
      Seq(null),
      Seq(),
      null
    )
    val rows = elements.map(el => Row.fromSeq(Seq(el)))
    val schema = StructType(Seq(StructField("a", ArrayType(sparkType), true)))
    val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)

    withTable(Seq(s"a Array($clickhouseTypeName)"), "a") {

      df.write.clickhouse(host, port, table)

      val dataFrame = sqlContext.read.clickhouse(host, port, table)

      dataFrame.schema.length should be(1)
      dataFrame.schema.head.dataType should be(ArrayType(clickhouseType.toSparkType(), false))

      val result: Array[java.util.List[clickhouseType.T]] = dataFrame
        .rdd
        .map(row => row.getList[clickhouseType.T](0))
        .collect()
        .sortBy(e => (e.size(), e.asScala.map(_.hashCode()).sum))

      val expected: Seq[Seq[clickhouseType.T]] = elements.map {
          case null => Seq[clickhouseType.T]()
          case arr: Seq[clickhouseType.T] => arr map {
            case null => typeDefaultValue
            case t => convertToOriginalType(t)
          }
        }
      .sortBy(e => (e.size, e.map(_.hashCode()).sum))

      result.length should be (expected.size)

      result.zip(expected).foreach { case (l, r) =>
        l.size() should be (r.size)
        l.asScala.zip(r).foreach { case (fromCh, original) =>
          assert(comparator(fromCh, original), s"Expected values: $original. Actual value: $fromCh")
        }
      }
    }

    withTable(Seq(s"a Array(Nullable($clickhouseTypeName))"), "tuple()") {
      val nullableComparator = (t: clickhouseType.T, s: Any) =>
        (t == null & s == null) || comparator(t, s.asInstanceOf[clickhouseType.T])

      df.write.clickhouse(host, port, table)

      val dataFrame = sqlContext.read.clickhouse(host, port, table)

      dataFrame.schema.length should be(1)
      dataFrame.schema.head.dataType should be(ArrayType(clickhouseType.toSparkType(), true))

      val result = dataFrame
        .rdd
        .map(row => row.getList[clickhouseType.T](0))
        .collect()
        .sortBy(e => (e.size(), e.asScala.map(el => Option(el).hashCode()).sum))

      val expected = elements.map {
        case null => Seq[clickhouseType.T]()
        case arr: Seq[clickhouseType.T] => arr map {
          case null => null
          case t => convertToOriginalType(t)
        }
      }.sortBy(e => (e.size, e.map(el => Option(el).hashCode()).sum))

        result.length should be (expected.size)
        result.zip(expected).foreach { case (l, r) =>
         l.size() should be (r.size)
         l.asScala.zip(r).foreach { case (fromCh, original) =>
           assert(nullableComparator(fromCh, original), s"Expected values: $original. Actual value: $fromCh")
         }
      }
    }
  }

  def testPrimitive(clickhouseType: ClickhouseType)(seq: Seq[Any],
                                                    rowConverter: Row => clickhouseType.T,
                                                    sparkType: DataType,
                                                    convertToOriginalType: Any => clickhouseType.T,
                                                    comparator: (clickhouseType.T, clickhouseType.T) => Boolean = (t: clickhouseType.T, s: clickhouseType.T) => t == s)
                   (implicit ord: Ordering[clickhouseType.T], ct: ClassTag[clickhouseType.T], sqlContext: SQLContext): Unit = {

    val clickhouseTypeName = clickhouseType.clickhouseDataTypeString

    val sc = sqlContext.sparkContext
    val rows = seq.map(el => Row.fromSeq(Seq(el)))
    val schema = StructType(Seq(StructField("a", sparkType, nullable = true)))

    val df = sqlContext.createDataFrame(sc.parallelize(rows), schema)
    withTable(Seq(s"a $clickhouseTypeName"), "a") {
      df.write.clickhouse(host, port, table)

      val rdd = sqlContext.read.clickhouse(host, port, table).rdd
      val res = rdd
        .map(row => rowConverter(row)).collect()

      val expected = seq.map{
        case null => clickhouseType.defaultValue
        case t => convertToOriginalType(t)
      }.sorted

      res.sorted.zip(expected).foreach { case (result, expected) =>
        assert(comparator(result, expected), s"Expected value: $expected. Actual value: $result")
      }
    }

    withTable(Seq(s"a Nullable($clickhouseTypeName)"), "tuple()") {
      df.write.clickhouse(host, port, table)

      val rdd = sqlContext.read.clickhouse(host, port, table).rdd

      val collect = rdd.collect()
      val nullsFromRdd = collect.count(_.isNullAt(0))
      val res = rdd.filter(row => !row.isNullAt(0)).map(row => rowConverter(row)).collect()

      val nullsFromExpected = seq.count(_ == null)
      val expected = seq.filter(_ != null).map(t => convertToOriginalType(t))
        .sorted

      assert(nullsFromRdd == nullsFromExpected, s"Number of null are not matched. Expected value: $nullsFromRdd, actual: $nullsFromRdd")

      res.sorted.zip(expected).foreach { case (result, expected) =>
        assert(comparator(result, expected), s"Expected value: $expected. Actual value: $result")
      }
    }
  }
}
