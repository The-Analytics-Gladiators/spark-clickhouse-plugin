package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseType
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.Properties

case class Field(name: String,
                 chType: ClickhouseType,
                 sparkDataType: DataType,
                 index: Int,
                 clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo)

case class ClickhouseWriterConfiguration(url: String,
                                         batchSize: Int,
                                         cluster: Option[String],
                                         schema: StructType,
                                         fields: Seq[Field],
                                         shardingStrategy: ShardingStrategy,
                                         connectionProps: Properties = new Properties())
