package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DoubleType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseFloat64(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override def toSparkType(): DataType = DoubleType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getDouble(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDouble(i + 1, row.getDouble(i))

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Float64
}
