package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, LongType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseInt64(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = Long
  override val defaultValue: Long = 0

  override def toSparkType(): DataType = LongType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getLong(name)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int64

  override protected def extractFromRow(i: Int, row: Row): Long = row.getLong(i)

  override protected def setValueToStatement(i: Int, value: Long, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setLong(i, value)
}
