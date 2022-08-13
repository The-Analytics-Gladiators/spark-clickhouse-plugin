package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseInt32(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override def toSparkType(): DataType = IntegerType

  override def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getInt(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setInt(i + 1, row.getInt(i))

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int32
}
