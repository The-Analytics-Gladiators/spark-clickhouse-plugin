package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.sql.types.{ClickhouseArray, ClickhouseDecimal, ClickhouseDecimalN, ClickhouseType, PrimitiveClickhouseType}
import com.clickhouse.client.ClickHouseDataType

object ClickhouseTypesParser {
  private val arrayPrefix = "Array("
  private val lowCardinalityPrefix = "LowCardinality("
  private val nullablePrefix = "Nullable("
  private val decimalPrefix = "Decimal"

  def parseType(typ: String, lowCardinality: Boolean = false, nullable: Boolean = false): ClickhouseType = {

    if (typ.startsWith(arrayPrefix)) {
      ClickhouseArray(parseType(typ.substring(arrayPrefix.length, typ.length - 1)))
    } else if (typ.startsWith(lowCardinalityPrefix)) {
      parseType(typ.substring(lowCardinalityPrefix.length, typ.length - 1), lowCardinality = true, nullable = nullable)
    } else if (typ.startsWith(nullablePrefix)) {
      parseType(typ.substring(nullablePrefix.length, typ.length - 1), lowCardinality = lowCardinality, nullable = true)
    } else {
      if (typ.startsWith(decimalPrefix)) {
        val rest = typ.substring(decimalPrefix.length, typ.length - 1)
        val bracketIndex = rest.indexOf('(')
        if (bracketIndex == 0) {
          val split = rest.substring(1).split(",")
          ClickhouseDecimal(split.head.trim.toInt, split(1).trim.toInt, nullable = nullable)
        } else {
          val n = rest.substring(0, bracketIndex).toInt
          val s = rest.substring(bracketIndex + 1).trim.toInt
          ClickhouseDecimalN(n, s, nullable = nullable)
        }
      } else {
        PrimitiveClickhouseType(ClickHouseDataType.valueOf(typ), nullable = nullable, lowCardinality = lowCardinality)
      }
    }
  }
}
