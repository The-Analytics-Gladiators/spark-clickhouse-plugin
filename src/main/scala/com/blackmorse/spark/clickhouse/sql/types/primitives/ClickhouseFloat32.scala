package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, FloatType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseFloat32(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = Float
  override val defaultValue: Float = 0.0f

  override def toSparkType(): DataType = FloatType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getFloat(name)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Float32

  override protected def extractFromRow(i: Int, row: Row): Float = row.getFloat(i)

  override protected def setValueToStatement(i: Int, value: Float, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setFloat(i, value)
}
