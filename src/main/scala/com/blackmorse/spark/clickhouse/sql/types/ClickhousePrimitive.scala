package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.primitives._
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.DataType

trait ClickhousePrimitive extends ClickhouseType {
  val nullable: Boolean
  val lowCardinality: Boolean

  def toSparkType(): DataType
  def clickhouseDataType: ClickHouseDataType

  override def clickhouseDataTypeString: String = clickhouseDataType.toString
}
