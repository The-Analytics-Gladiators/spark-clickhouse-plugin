package com.blackmorse.spark.clickhouse.sql.types

import com.clickhouse.client.ClickHouseDataType

sealed trait ClickhouseType

case class PrimitiveClickhouseType(typ: ClickHouseDataType, nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType

sealed trait DecimalTrait extends ClickhouseType

/**
 * Responsible for Decimal(P, S) Clickhouse type
 */
case class ClickhouseDecimal(p: Int, s: Int, nullable: Boolean) extends DecimalTrait

/**
 * Responsible for Decimal32(S), Decimal64(S), Decimal128(S) and Decimal256(S) Clickhouse types
 */
case class ClickhouseDecimalN(n: Int, s: Int, nullable: Boolean) extends DecimalTrait

case class ClickhouseDateTime64(p: Int, nullable: Boolean) extends ClickhouseType

case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType
case class ClickhouseMap(key: ClickhouseType, value: ClickhouseType, nullable: Boolean) extends ClickhouseType

case class ClickhouseField(name: String, typ: ClickhouseType)
