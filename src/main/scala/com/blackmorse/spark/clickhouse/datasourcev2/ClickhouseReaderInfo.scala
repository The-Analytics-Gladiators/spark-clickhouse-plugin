package com.blackmorse.spark.clickhouse.datasourcev2

import org.apache.spark.sql.types.StructType

import java.sql.ResultSet

case class ClickhouseReaderInfo(schema: StructType,
                                tableName: String,
                                url: String,
                                rowMapper: ResultSet => Seq[Any])
