package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{SimpleArrayRSExtractor, StandardInternalRowConverter, StringRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

import java.sql.PreparedStatement

//TODO Not used yet
case class ClickhouseMap(key: ClickhouseType, value: ClickhouseType, nullable: Boolean)
    extends ClickhouseType
    with StringRSExtractor
    with SimpleArrayRSExtractor {
  override type T = Map[key.T, value.T]

  override def toSparkType: DataType = ???

  override val defaultValue: Map[key.T, value.T] = Map()

  override def setValueToStatement(i: Int, v: Map[key.T, value.T], statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = ???

  override def clickhouseDataTypeString: String = ???

  override def convertInternalValue(value: Any): Map[key.T, ClickhouseMap.this.value.T] = ???

  override def convertInternalArrayValue(value: ArrayData, sparkType: DataType): Seq[Map[key.T, ClickhouseMap.this.value.T]] = ???
}
