package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{IntRSExtractor, SimpleArrayRSExtractor}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, ShortType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseUInt16(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with IntRSExtractor
    with SimpleArrayRSExtractor {
  override type T = Int
  override val defaultValue: Int = 0

  override def toSparkType(): DataType = IntegerType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt16

  override protected def setValueToStatement(i: Int, value: Int, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setInt(i, value)
}

object ClickhouseUInt16 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Int = (row, index) => sparkType match {
    case ByteType    => row.getByte(index).toInt
    case ShortType   => row.getShort(index).toInt
    case IntegerType => row.getInt(index)
  }
}