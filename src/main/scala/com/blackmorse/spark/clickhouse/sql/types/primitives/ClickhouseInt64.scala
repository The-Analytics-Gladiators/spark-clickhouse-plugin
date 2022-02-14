package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, LongType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseInt64(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override def toSparkType(): DataType = LongType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getLong(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setLong(i + 1, row.getLong(i))

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int64
}
