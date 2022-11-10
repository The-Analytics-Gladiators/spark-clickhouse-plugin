package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{ShortRSExtractor, SimpleArrayRSExtractor}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, ShortType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseUInt8(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with ShortRSExtractor
    with SimpleArrayRSExtractor {
  override type T = Short
  override lazy val defaultValue: Short = 0

  override def toSparkType(): DataType = ShortType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt8

  override protected def setValueToStatement(i: Int, value: Short, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setShort(i, value)
}

object ClickhouseUInt8 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Short = (row, index) => sparkType match {
    case ByteType  => row.getByte(index).toShort
    case ShortType => row.getShort(index)
  }
}