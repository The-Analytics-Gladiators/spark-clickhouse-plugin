package com.blackmorse.spark.clickhouse.sql.types

import org.apache.spark.sql.types.DataType

import java.sql.ResultSet

trait ClickhouseType {
  val nullable: Boolean
  def toSparkType(): DataType
  def extractFromRs(name: String, resultSet: ResultSet): Any
}




case class ClickhouseField(name: String, typ: ClickhouseType) {
  def extractFromRs(resultSet: ResultSet): Any = {
    typ.extractFromRs(name, resultSet)
  }
}
