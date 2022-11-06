package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DoubleRSExtractor, SimpleArrayRSExtractor}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseFloat64(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DoubleRSExtractor
    with SimpleArrayRSExtractor {
  override type T = Double
  override val defaultValue: Double = 0.0d

  override def toSparkType(): DataType = DoubleType

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Float64

  override protected def setValueToStatement(i: Int, value: Double, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDouble(i, value)
}

object ClickhouseFloat64 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Double = (row, index) => sparkType match {
    case FloatType  => row.getFloat(index).toDouble
    case DoubleType => row.getDouble(index)
  }
}