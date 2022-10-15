package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.sql.types._
import com.blackmorse.spark.clickhouse.sql.types.arrays.{ArraySupport, DateTimeArraySupport, DecimalArraySupport, NotImplementedArraySupport}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType

import java.sql.ResultSet


object ClickhouseTypesParser {
  private val arrayPrefix = "Array("
  private val lowCardinalityPrefix = "LowCardinality("
  private val nullablePrefix = "Nullable("
  private val decimalPrefix = "Decimal("
  private val dateTime = "DateTime"
  private val dateTime64 = "DateTime64("

  def parseType(typ: String, lowCardinality: Boolean = false, nullable: Boolean = false): ClickhouseType with ArraySupport = {

    if (typ.startsWith(arrayPrefix)) {
      new ClickhouseArray(parseType(typ.substring(arrayPrefix.length, typ.length - 1))) with NotImplementedArraySupport
    } else if (typ.startsWith(lowCardinalityPrefix)) {
      parseType(typ.substring(lowCardinalityPrefix.length, typ.length - 1), lowCardinality = true, nullable = nullable)
    } else if (typ.startsWith(nullablePrefix)) {
      parseType(typ.substring(nullablePrefix.length, typ.length - 1), lowCardinality = lowCardinality, nullable = true)
    } else if (typ.startsWith(dateTime64)) {
      val p = typ.substring(dateTime64.length, typ.length - 1).trim.toInt
      new ClickhouseDateTime64(p, nullable = nullable) with DateTimeArraySupport
    } else if (typ.startsWith(dateTime)) {
      new ClickhouseDateTime(nullable = nullable, lowCardinality = lowCardinality) with DateTimeArraySupport
    } else if (typ.startsWith(decimalPrefix)) {
      val rest = typ.substring(decimalPrefix.length, typ.length - 1)
      val split = rest.split(",")
      new ClickhouseDecimal(split.head.trim.toInt, split(1).trim.toInt, nullable = nullable) with DecimalArraySupport
    } else {
        ClickhousePrimitive.toPrimitiveConstructor(ClickHouseDataType.valueOf(typ))(nullable, lowCardinality)
    }
  }
}
