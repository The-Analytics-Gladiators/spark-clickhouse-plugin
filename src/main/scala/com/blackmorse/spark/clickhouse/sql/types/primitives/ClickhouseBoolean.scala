package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{BooleanRSExtractor, SimpleArrayRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{BooleanType, DataType}

import java.sql.PreparedStatement

case class ClickhouseBoolean(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with BooleanRSExtractor
    with SimpleArrayRSExtractor {
  override type T = Boolean
  override def toSparkType(): DataType = BooleanType
  override val defaultValue: Boolean = false

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.Bool

  override protected def setValueToStatement(i: Int, value: Boolean, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBoolean(i, value)

  //Impossible to set Array[AnyRef] with Booleans for JDBC driver
  override def mapFromArray(value: Any): AnyRef = (value match {
    case null => if(nullable) null else 0.toByte
    case false => 0.toByte
    case true => 1.toByte
    case b: Byte => if (b == 0) 0 else 1
    case b: Short => if (b == 0) 0 else 1
    case b: Int =>if (b == 0) 0 else 1
    case b: Long => if (b == 0) 0 else 1
  }).asInstanceOf[AnyRef]
}
