package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseUInt32(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = Long
  override val defaultValue: Long = 0

  override def toSparkType(): DataType = LongType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getLong(name)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt32

  override protected def setValueToStatement(i: Int, value: Long, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setLong(i, value)
}

object ClickhouseUInt32 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Long = (row, index) => sparkType match {
    case ByteType    => row.getByte(index).toLong
    case ShortType   => row.getShort(index).toLong
    case IntegerType => row.getInt(index).toLong
    case LongType    => row.getLong(index)
  }
}