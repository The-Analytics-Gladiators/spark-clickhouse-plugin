package com.blackmorse.spark.clickhouse.write

import com.blackmorse.spark.clickhouse.ClickhouseHosts._
import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, StructField, StructType, TimestampType}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{Date, Timestamp}
import scala.collection.immutable.Seq


class DatesBackwardCompatibilityTest extends AnyFlatSpec with DataFrameSuiteBase {

  import com.blackmorse.spark.clickhouse._

  private val nowTimestamp: Long = System.currentTimeMillis()

  "Date" should "write in ch DateTime column" in {
    withTable(Seq("a DateTime"), "a") {
      val storedDate = new Date(nowTimestamp)
      val expectedDate = storedDate.toLocalDate.toString + " 00:00:00.0"

      val schema = StructType(Seq(StructField("a", DateType, nullable = true)))
      val data = Seq(Row(storedDate))
      val df = sqlContext.createDataFrame(sc.parallelize(data), schema)

      df.write.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val resDf = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val result = resDf.collect().map(_.getTimestamp(0)).map(_.toString)

      assert(result sameElements Array(expectedDate))
    }
  }

  "DateTime" should "write in ch date column" in {
    withTable(Seq("a date"), "a") {
      val storedDate = new Timestamp(nowTimestamp)
      val expectedDate = storedDate.toLocalDateTime.toLocalDate.toString
      val schema = StructType(Seq(StructField("a", TimestampType, nullable = true)))
      val data = Seq(Row(storedDate))
      val df = sqlContext.createDataFrame(sc.parallelize(data), schema)

      df.write.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val resDf = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)

      val result = resDf.collect().map(_.getDate(0)).map(_.toString)

      assert(result sameElements Array(expectedDate))
    }
  }

}
