package com.blackmorse.spark.clickhouse.sql.types

import com.clickhouse.client.ClickHouseDataType

trait ClickhousePrimitive extends ClickhouseType {
  val lowCardinality: Boolean

  def clickhouseDataType: ClickHouseDataType

  override def clickhouseDataTypeString: String = clickhouseDataType.toString
}
