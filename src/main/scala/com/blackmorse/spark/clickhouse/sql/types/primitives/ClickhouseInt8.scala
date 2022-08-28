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

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getByte(name)

  override def extractFromRow(i: Int, row: Row): Byte = row.getByte(i)

  override protected def setValueToStatement(i: Int, value: Byte, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setByte(i, value)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int8
}
