package com.blackmorse.spark.clickhouse

import com.clickhouse.jdbc.ClickHouseDriver
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec
import com.blackmorse.spark.clickhouse.ClickhouseHosts._

import java.util.Properties

class DummyTest extends AnyFlatSpec with DataFrameSuiteBase {
  "spark" should "run" in {
     val set = sc.parallelize(Seq(1, 2, 3, 4))
       .collect().toSet

    assert(set == Set(1, 2, 3, 4))
  }

  "Test clickhouse cluster" should "be consistent" in {
  //TODO managed connections
    val connection = new ClickHouseDriver().connect(s"jdbc:clickhouse://$shard1Replica1", new Properties());
    val connection2 = new ClickHouseDriver().connect(s"jdbc:clickhouse://$shard2Replica1", new Properties());

    connection.createStatement().executeQuery(s"CREATE TABLE t ON CLUSTER  $clusterName (a UInt64) ENGINE = MergeTree() ORDER BY a")
    connection.createStatement().executeQuery(s"CREATE TABLE d ON CLUSTER  $clusterName (a UInt64) ENGINE = Distributed('$clusterName', 'default', 't')")
    connection.createStatement().executeQuery("INSERT INTO t VALUES (1)")
    connection2.createStatement().executeQuery("INSERT INTO t VALUES (2)")


    val frame1 = sqlContext.read.clickhouse(shard1Replica1.hostName, shard1Replica1.port, "default.d")
    val set = frame1.collect().map(_.getDecimal(0)).map(_.longValue()).toSet

    assert(set == Set(1L, 2L))

    connection.createStatement().executeQuery(s"DROP TABLE t ON CLUSTER $clusterName")
    connection.createStatement().executeQuery(s"DROP TABLE d ON CLUSTER $clusterName")

    connection.close()
    connection2.close()
  }
}
