package com.blackmorse.spark.clickhouse.reader

import com.clickhouse.client.ClickHouseDataType

object ClickhouseTypesParser {
  private val arrayPrefix = "Array("
  private val lowCardinalityPrefix = "LowCardinality("
  private val nullablePrefix = "Nullable("

  def parseType(typ: String, lowCardinality: Boolean = false, nullable: Boolean = false): ClickhouseType = {

    if (typ.startsWith(arrayPrefix)) {
      ClickhouseArray(parseType(typ.substring(arrayPrefix.length, typ.length - 1)))
    } else if (typ.startsWith(lowCardinalityPrefix)) {
      parseType(typ.substring(lowCardinalityPrefix.length, typ.length - 1), lowCardinality = true, nullable = nullable)
    } else if (typ.startsWith(nullablePrefix)) {
      parseType(typ.substring(nullablePrefix.length, typ.length - 1), lowCardinality = lowCardinality, nullable = true)
    } else {
      PrimitiveClickhouseType(ClickHouseDataType.valueOf(typ), nullable = nullable, lowCardinality = lowCardinality)
    }
  }
}
