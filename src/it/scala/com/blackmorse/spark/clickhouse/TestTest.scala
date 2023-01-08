package com.blackmorse.spark.clickhouse

import com.clickhouse.jdbc.ClickHouseDataSource
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Properties

class TestTest extends AnyFlatSpec {
  it should "asdsad" in {
//    val driver = new ClickHouseDriver()
    val props = new Properties()
//    props.setProperty("insert_quorum", "3")
      props.setProperty("custom_http_params", "insert_quorum=2,insert_deduplicate=0,max_insert_block_size=10000000")
//    props.setProperty("user", "default")



//    val connection = driver.connect()
//    val clickhouseDS = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123?insert_quorum=3", props)
    val clickhouseDS = new ClickHouseDataSource("jdbc:clickhouse://localhost:8123?", props)
    val connection = clickhouseDS.getConnection()

    val statement = connection.prepareStatement("INSERT INTO default.aaa values (?)")

    statement.setLong(1, 13L)

    statement.addBatch()
    statement.executeBatch()


  }

}
