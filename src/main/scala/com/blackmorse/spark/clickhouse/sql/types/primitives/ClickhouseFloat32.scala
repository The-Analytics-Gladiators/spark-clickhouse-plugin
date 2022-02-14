package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, FloatType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseFloat32(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override def toSparkType(): DataType = FloatType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getFloat(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setFloat(i + 1, row.getFloat(i))

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Float32
}
