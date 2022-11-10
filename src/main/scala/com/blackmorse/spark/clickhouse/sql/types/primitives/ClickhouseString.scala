package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{StringArrayRSExtractor, StringRSExtractor}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType}

import java.sql.PreparedStatement

case class ClickhouseString(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with StringRSExtractor
    with StringArrayRSExtractor {
  override type T = String
  override val defaultValue: String = ""

  override def toSparkType(): DataType = StringType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.String

  override protected def setValueToStatement(i: Int, value: String, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setString(i, value)
}

object ClickhouseString {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => String = (row, index) => sparkType match {
    case StringType => row.getString(index)
  }
}