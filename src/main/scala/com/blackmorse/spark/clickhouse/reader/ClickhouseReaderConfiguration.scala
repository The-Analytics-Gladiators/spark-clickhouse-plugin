package com.blackmorse.spark.clickhouse.reader

import org.apache.spark.sql.types.StructType

import java.sql.ResultSet

case class ClickhouseReaderConfiguration(schema: StructType,
                                         tableName: String,
                                         url: String,
                                         rowMapper: ResultSet => Seq[Any])
