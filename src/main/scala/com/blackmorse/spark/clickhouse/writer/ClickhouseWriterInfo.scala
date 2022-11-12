package com.blackmorse.spark.clickhouse.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import java.sql.PreparedStatement
import java.util.Properties

case class ClickhouseWriterInfo(url: String,
                                tableName: String,
                                batchSize: Int,
                                schema: StructType,
                                rowSetters: Seq[(InternalRow, PreparedStatement) => Unit],
                                connectionProperties: Properties = new Properties())
