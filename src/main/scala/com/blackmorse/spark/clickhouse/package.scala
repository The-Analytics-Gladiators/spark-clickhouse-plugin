package com.blackmorse.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object clickhouse {
  val CLICKHOUSE_HOST_NAME = "HOST_NAME"
  val CLICKHOUSE_PORT = "PORT"
  val TABLE = "table"

  implicit class ClickHouseDataWriter[T](writer: DataFrameWriter[T]) {
    def clickhouse(host: String, port: Int, table: String): Unit = {
      writer
        .format("com.blackmorse.spark.clickhouse")
        .option(CLICKHOUSE_HOST_NAME, host)
        .option(CLICKHOUSE_PORT, port)
        .option(TABLE, table)
        .save()
    }
  }

  implicit class ClickHouseDataFrameReader(reader: DataFrameReader) {
    def clickhouse(host: String, port: Int, table: String): DataFrame =
      reader
        .format("com.blackmorse.spark.clickhouse")
        .option(CLICKHOUSE_HOST_NAME, host)
        .option(CLICKHOUSE_PORT, port)
        .option(TABLE, table)
        .load()
  }
}
