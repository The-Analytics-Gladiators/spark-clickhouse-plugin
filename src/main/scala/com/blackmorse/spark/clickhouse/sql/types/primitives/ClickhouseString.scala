package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StringType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseString(nullable: Boolean, lowCardinality: Boolean) extends ClickhousePrimitive {
  override def toSparkType(): DataType = StringType

  override def extractFromRs(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getString(name)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setString(i + 1, row.getString(i))

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.String
}
