package com.blackmorse.spark.clickhouse.sql.types

import org.apache.spark.sql.types.DataType

import java.sql.ResultSet

case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType {
  override def toSparkType(): DataType = ???
  override val nullable: Boolean = false

  override def extractFromRs(name: String, resultSet: ResultSet): Any = ???
}
