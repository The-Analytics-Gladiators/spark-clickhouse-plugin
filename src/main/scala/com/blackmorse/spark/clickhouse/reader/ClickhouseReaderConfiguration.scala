package com.blackmorse.spark.clickhouse.reader

import org.apache.spark.sql.types.StructType

import java.sql.ResultSet
import java.util.Properties

case class ClickhouseReaderConfiguration(schema: StructType,
                                         url: String,
                                         cluster: Option[String],
                                         rowMapper: ResultSet => Seq[Any],
                                         connectionProps: Properties)

//TODO Split into MergeTree, Distributed, etc
case class TableInfo(name: String,
                     engine: String,
                     cluster: Option[String],
                     orderingKey: Option[String])
