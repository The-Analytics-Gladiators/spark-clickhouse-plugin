package com.blackmorse.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SaveMode}

package object clickhouse {
  val CLICKHOUSE_HOST_NAME = "HOST_NAME"
  val CLICKHOUSE_PORT = "PORT"
  val TABLE = "TABLE"
  val BATCH_SIZE = "BATCH_SIZE"

  implicit class ClickHouseDataWriter[T](writer: DataFrameWriter[T]) {
    def clickhouse(host: String, port: Int, table: String): Unit = {
      writer
        .format("com.blackmorse.spark.clickhouse")
        .option(CLICKHOUSE_HOST_NAME, host)
        .option(CLICKHOUSE_PORT, port)
        .option(TABLE, table)
        .option(BATCH_SIZE, 1000000)
//        .option("spark.sql.storeAssignmentPolicy", "legacy")
        .mode(SaveMode.Append)
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
