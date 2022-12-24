package com.blackmorse.spark.clickhouse.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.sql.PreparedStatement
import java.util.Properties

case class ClickhouseWriterConfiguration(url: String,
                                         batchSize: Int,
                                         cluster: Option[String],
                                         schema: StructType,
                                         rowSetters: Seq[(InternalRow, PreparedStatement) => Unit],
                                         connectionProps: Properties = new Properties())
