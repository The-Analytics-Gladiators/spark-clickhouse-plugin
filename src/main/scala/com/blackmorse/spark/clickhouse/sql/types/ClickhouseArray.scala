package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.sql.types.extractors.{ArrayInternalRowConverter, ArrayRSExtractor, SimpleArrayRSExtractor, StandardRowArrayConverter}
import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import org.apache.spark.sql.types.{ArrayType, DataType}

import java.sql.PreparedStatement

case class ClickhouseArray(typ: ClickhouseType)
    extends ClickhouseType
    with ArrayRSExtractor
    with SimpleArrayRSExtractor
    with ArrayInternalRowConverter
    with StandardRowArrayConverter[Seq[AnyRef]] {
  override type T = Seq[AnyRef]
  override lazy val defaultValue: T = Seq[AnyRef]()

  override def toSparkType: DataType = ArrayType(typ.toSparkType, typ.nullable)
  override val nullable: Boolean = false

  override def setValueToStatement(i: Int, value: Seq[AnyRef], statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    val array = value.map {
      case null => if (typ.nullable) null else typ.defaultValue.asInstanceOf[AnyRef]
      case v => v
    }
    val jdbcArray = statement.getConnection.createArrayOf(clickhouseDataTypeString, array.toArray)
    statement.setArray(i, jdbcArray)
  }

  override def clickhouseDataTypeString: String = s"Array(${typ.clickhouseDataTypeString})"
}