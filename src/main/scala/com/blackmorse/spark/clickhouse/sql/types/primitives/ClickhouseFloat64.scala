package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DoubleType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseFloat64(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override type T = Double
  override val defaultValue: Double = 0.0d

  override def toSparkType(): DataType = DoubleType

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getDouble(name)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Float64

  override protected def setValueToStatement(i: Int, value: Double, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDouble(i, value)
}

object ClickhouseFloat64 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Any = sparkType match {
    case DoubleType => (row, index) => row.getDouble(index)
  }
}