package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseInt8(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = Byte
  override lazy val defaultValue: Byte = 0

  override def toSparkType(): DataType = ByteType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getByte(name)

  override protected def setValueToStatement(i: Int, value: Byte, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setByte(i, value)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int8
}

object ClickhouseInt8 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Byte = (row, index) => sparkType match {
    case ByteType => row.getByte(index)
  }
}