package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseInt8(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override def toSparkType(): DataType = ByteType

  override def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getByte(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setByte(i + 1, row.getByte(i))

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Int8
}
