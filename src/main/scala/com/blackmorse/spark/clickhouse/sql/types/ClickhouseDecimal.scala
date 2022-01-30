package com.blackmorse.spark.clickhouse.sql.types

import org.apache.spark.sql.types.{DataType, DecimalType}

import java.sql.ResultSet

sealed trait DecimalTrait extends ClickhouseType

/**
 * Responsible for Decimal(P, S) Clickhouse type
 */
case class ClickhouseDecimal(p: Int, s: Int, nullable: Boolean) extends DecimalTrait {
  override def toSparkType(): DataType = DecimalType(p, s)

  override def extractFromRs(name: String, resultSet: ResultSet): Any = resultSet.getBigDecimal(name)
}