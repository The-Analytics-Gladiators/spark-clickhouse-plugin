package com.blackmorse.spark.clickhouse

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Properties
import java.util.concurrent.TimeUnit
import ru.yandex.clickhouse.BalancedClickhouseDataSource

class DummyTest extends AnyFlatSpec with DataFrameSuiteBase {
  "spark" should "run" in {
     val set = sc.parallelize(Seq(1, 2, 3, 4))
       .collect().toSet

    assert(set == Set(1, 2, 3, 4))
  }

  "Test clickhouse cluster" should "be consistent" in {
  //TODO managed connections
    val dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://localhost:8123", new Properties());
    val dataSource2 = new BalancedClickhouseDataSource("jdbc:clickhouse://localhost:8124", new Properties());
    val connection = dataSource.getConnection()
    val connection2 = dataSource2.getConnection()
    
    connection.createStatement().executeQuery("CREATE TABLE t ON CLUSTER  spark_clickhouse_cluster (a UInt64) ENGINE = MergeTree() ORDER BY a")
    connection.createStatement().executeQuery("CREATE TABLE d ON CLUSTER  spark_clickhouse_cluster (a UInt64) ENGINE = Distributed('spark_clickhouse_cluster', 'default', 't')")
    connection.createStatement().executeQuery("INSERT INTO t VALUES (1)")
    connection2.createStatement().executeQuery("INSERT INTO t VALUES (2)")


    val frame1 = sqlContext.read.jdbc("jdbc:clickhouse://localhost:8123", "default.d", new Properties())
    val set = frame1.collect().map(_.getDecimal(0)).map(_.longValue()).toSet

    assert(set == Set(1L, 2L))

    connection.close()
    connection2.close()
  }
}
