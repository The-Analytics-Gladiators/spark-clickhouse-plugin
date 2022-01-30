package com.blackmorse.spark.clickhouse.sql.types

import org.apache.spark.sql.types.{DataType, TimestampType}

import java.sql.ResultSet

case class ClickhouseDateTime(nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = TimestampType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getTimestamp(name)
}

case class ClickhouseDateTime64(p: Int, nullable: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = TimestampType

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getTimestamp(name)
}
