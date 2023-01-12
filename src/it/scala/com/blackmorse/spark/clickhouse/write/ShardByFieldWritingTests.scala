package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseHosts.{clusterDistributedTestTable, clusterName, clusterTestTable, shard1Replica1, shard2Replica1}
import com.blackmorse.spark.clickhouse.ClickhouseTests.withClusterTable
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Encoder, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}
import scala.reflect.ClassTag

class ShardByFieldWritingTests extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  private def testWithType[T](typeName: String, fromIntFunc: Int => T, fromRowFunc: Row => T)
                             (implicit encoder: Encoder[T], ord: Ordering[T], t: ClassTag[T]): Unit = {
    withClusterTable(Seq(s"a $typeName"), "a", true) {

      import sqlContext.implicits._

      val data = (1 to 15).map(i => Seq.fill(2)(fromIntFunc(i))).reduce(_ ++ _).sorted

      val value1 = spark.sparkContext.parallelize(data)
      value1.toDF("a")
        .write
        .shardBy("a")
        .option("insert_deduplicate", 0)
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterName, clusterDistributedTestTable)

      val resultShard1 = spark
        .read
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, clusterTestTable)
        .collect()
        .map(fromRowFunc)

      val resultShard2 = spark
        .read
        .clickhouse(shard2Replica1.hostName, shard2Replica1.port, clusterTestTable)
        .collect()
        .map(fromRowFunc)

      resultShard1.intersect(resultShard2) shouldEqual Array[T]()

      (resultShard1 ++ resultShard2).sorted shouldEqual data
    }
  }

  it should "shard by different types of fields" in {
    import sqlContext.implicits._
    implicit val ord: Ordering[Date] = Ordering.by(_.getTime)
    implicit val ordTimestamp: Ordering[Timestamp] = Ordering.by(_.getTime)

    testWithType("Int32", identity, _.getInt(0))
    testWithType("String", _.toString, _.getString(0))
    testWithType("Date", i => DateTimeUtils.date(i), _.getDate(0))
    testWithType("DateTime", i => DateTimeUtils.time(i), _.getTimestamp(0))
    testWithType("Bool", i => if (i % 2 ==0) true else false, _.getBoolean(0))
    testWithType("Decimal(5, 5)", _.toString, _.getString(0).toDouble.toInt.toString /* cutting trailing zeros */)
  }
}
