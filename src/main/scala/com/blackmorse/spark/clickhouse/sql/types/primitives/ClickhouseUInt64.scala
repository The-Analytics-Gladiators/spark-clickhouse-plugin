package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DecimalRSExtractor, UInt64ArrayRSExtractor}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseUInt64(nullable: Boolean, lowCardinality: Boolean)
    extends ClickhousePrimitive
    with DecimalRSExtractor
    with UInt64ArrayRSExtractor {
  override type T = java.math.BigDecimal
  override val defaultValue: java.math.BigDecimal = java.math.BigDecimal.ZERO

  override def toSparkType(): DataType = DecimalType(38, 0)

  override def clickhouseDataType: ClickHouseDataType = ClickHouseDataType.UInt64

  override def extractArrayFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): AnyRef = {
    val array = resultSet.getArray(name).getArray
    //Array(Nullable(primitive)) produces Long[], while Array(primitive) -> long[]
    if(nullable) {
      val mapper = (l: java.lang.Long) => if (l == null) null else Decimal(l)
      array.asInstanceOf[Array[java.lang.Long]].map(mapper)
    } else {
      val mapper = (l: Long) => Decimal(l)
      array.asInstanceOf[Array[Long]].map(mapper)
    }
  }

  override protected def setValueToStatement(i: Int, value: java.math.BigDecimal, statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    statement.setBigDecimal(i, value)

  override def convertInternalValue(value: Any): java.math.BigDecimal =
    value.asInstanceOf[Decimal].toJavaBigDecimal

  override def mapFromArray(value: Any): AnyRef =
    if (value == null) null else value.asInstanceOf[Decimal].toJavaBigDecimal
}