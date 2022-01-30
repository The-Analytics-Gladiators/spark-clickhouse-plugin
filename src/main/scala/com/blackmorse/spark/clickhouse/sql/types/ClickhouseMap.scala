package com.blackmorse.spark.clickhouse.sql.types

import org.apache.spark.sql.types.DataType

import java.sql.ResultSet

case class ClickhouseMap(key: ClickhouseType, value: ClickhouseType, nullable: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = ???

  override def extractFromRs(name: String, resultSet: ResultSet): Any = ???
}
