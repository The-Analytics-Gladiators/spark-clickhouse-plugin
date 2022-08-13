package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DateType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseDate32(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override def toSparkType(): DataType = DateType

  override def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getDate(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setDate(i + 1, row.getDate(i))

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Date32
}
