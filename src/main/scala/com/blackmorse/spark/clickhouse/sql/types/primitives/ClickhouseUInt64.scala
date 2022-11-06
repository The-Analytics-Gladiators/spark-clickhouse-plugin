package com.blackmorse.spark.clickhouse.sql.types.primitives

import com.blackmorse.spark.clickhouse.sql.types.ClickhousePrimitive
import com.blackmorse.spark.clickhouse.sql.types.extractors.{DecimalRSExtractor, UInt64ArrayRSExtractor}
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, Decimal, DecimalType, IntegerType, LongType, ShortType}

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
}

object ClickhouseUInt64 {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => java.math.BigDecimal = (row, index) => sparkType match {
    case ByteType      => new java.math.BigDecimal(row.getByte(index))
    case ShortType     => new java.math.BigDecimal(row.getShort(index))
    case IntegerType   => new java.math.BigDecimal(row.getInt(index))
    case LongType      => new java.math.BigDecimal(row.getLong(index))
    case DecimalType() => row.getDecimal(index)
  }
}