package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, ShortType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseUInt16(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = Int
  override val defaultValue: Int = 0

  override def toSparkType(): DataType = IntegerType

  protected override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getInt(name)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt16

  override protected def setValueToStatement(i: Int, value: Int, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setInt(i, value)
}