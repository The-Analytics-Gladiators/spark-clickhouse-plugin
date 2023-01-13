package com.blackmorse.spark.clickhouse.utils

import com.blackmorse.spark.clickhouse.ClickhouseHosts.{shard1Replica1, testTable}
import com.blackmorse.spark.clickhouse.ClickhouseTests.withTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.blackmorse.spark.clickhouse._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class ClickhousePropertiesTests extends AnyFlatSpec with Matchers with DataFrameSuiteBase {
  "Username" should "be passed by option" in {
    assertThrows[Exception] {
      spark.read
        .option("user", "some_user")
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, "system.clusters")
    }
  }

  "Password" should "be passed by option" in {
    assertThrows[Exception] {
      spark.read
        .option("password", "some_password")
        .clickhouse(shard1Replica1.hostName, shard1Replica1.port, "system.clusters")
    }
  }

  "insert_quorum" should "be passed by jdbcParam" in {
    withTable(Seq("a Int64"), "a") {
      import spark.sqlContext.implicits._
      assertThrows[Exception] {
        spark.sparkContext.parallelize(Seq(1, 2, 3))
          .toDF("a")
          .write
          .jdbcParam("insert_quorum", "3")
          .clickhouse(shard1Replica1.hostName, shard1Replica1.port, testTable)
      }
    }
  }
}
