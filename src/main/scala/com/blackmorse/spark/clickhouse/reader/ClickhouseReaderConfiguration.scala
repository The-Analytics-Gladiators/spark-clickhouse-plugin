package com.blackmorse.spark.clickhouse.reader

import org.apache.spark.sql.types.StructType

import java.sql.ResultSet
import java.util.Properties

case class ClickhouseReaderConfiguration(schema: StructType,
                                         tableName: String,
                                         engine: String,
                                         url: String,
                                         cluster: Option[String],
                                         rowMapper: ResultSet => Seq[Any],
                                         connectionProperties: Properties)
