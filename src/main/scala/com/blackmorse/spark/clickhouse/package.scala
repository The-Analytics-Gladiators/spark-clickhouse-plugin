package com.blackmorse.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SaveMode}

package object clickhouse {
  val CLICKHOUSE_HOST_NAME = "HOST_NAME"
  val CLICKHOUSE_PORT = "PORT"
  val TABLE = "TABLE"
  val BATCH_SIZE = "BATCH_SIZE"
  val CLUSTER = "CLUSTER"
  val READ_DIRECTLY_FROM_DISTRIBUTED_TABLE = "read_directly_from_distributed"
  val DIRECTLY_USE_DISTRIBUTED_TABLE = "read_directly_from_distributed"
  val RANDOM_WRITES_SHUFFLE = "random_writes_shuffle"
  val USE_FORCE_COLLAPSING_MODIFIER = "use_force_collapsing_modifier"
  

  implicit class ClickHouseDataWriter[T](writer: DataFrameWriter[T]) {
    def clickhouse(host: String, port: Int, table: String): Unit = {
      writer
        .format("com.blackmorse.spark.clickhouse")
        .option(CLICKHOUSE_HOST_NAME, host)
        .option(CLICKHOUSE_PORT, port)
        .option(TABLE, table)
        .mode(SaveMode.Append)
        .save()
    }

    def clickhouse(host: String, port: Int, cluster: String, table: String): Unit = {
      writer
        .format("com.blackmorse.spark.clickhouse")
        .option(CLICKHOUSE_HOST_NAME, host)
        .option(CLICKHOUSE_PORT, port)
        .option(TABLE, table)
        .option(CLUSTER, cluster)
        .mode(SaveMode.Append)
        .save()
    }

    def writeDirectlyToDistributedTable(): DataFrameWriter[T] =
      writer.option(DIRECTLY_USE_DISTRIBUTED_TABLE, true)

    def shuffle(): DataFrameWriter[T] =
      writer.option(RANDOM_WRITES_SHUFFLE, true)

    def batchSize(size: Int): DataFrameWriter[T] =
      writer.option(BATCH_SIZE, size)
  }

  implicit class ClickHouseDataFrameReader(reader: DataFrameReader) {
    def clickhouse(host: String, port: Int, table: String): DataFrame =
      reader
        .format("com.blackmorse.spark.clickhouse")
        .option(CLICKHOUSE_HOST_NAME, host)
        .option(CLICKHOUSE_PORT, port)
        .option(TABLE, table)
        .load()

    def clickhouse(host: String, port: Int, cluster: String, table: String): DataFrame =
      reader
        .format("com.blackmorse.spark.clickhouse")
        .option(CLICKHOUSE_HOST_NAME, host)
        .option(CLICKHOUSE_PORT, port)
        .option(TABLE, table)
        .option(CLUSTER, cluster)
        .load()

    def readDirectlyFromDistributedTable(): DataFrameReader =
      reader.option(DIRECTLY_USE_DISTRIBUTED_TABLE, true)

    def batchSize(size: Int): DataFrameReader =
      reader.option(BATCH_SIZE, size)

    def disableForceCollapsing(): DataFrameReader =
      reader.option(USE_FORCE_COLLAPSING_MODIFIER, false)
  }
}
