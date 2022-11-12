package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.utils.ClickhouseTimeZoneInfo
import com.blackmorse.spark.clickhouse.sql.types.extractors.{ArrayRSExtractor, SimpleArrayRSExtractor}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType}

import java.sql.PreparedStatement

case class ClickhouseArray(typ: ClickhouseType)
    extends ClickhouseType
    with ArrayRSExtractor
    with SimpleArrayRSExtractor {
  override type T = Seq[AnyRef]
  override lazy val defaultValue: T = Seq[AnyRef]()

  override def toSparkType(): DataType = ArrayType(typ.toSparkType(), typ.nullable)
  override val nullable: Boolean = false

  override protected def setValueToStatement(i: Int, value: Seq[AnyRef], statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    val array = value map typ.mapFromArray
    val jdbcArray = statement.getConnection.createArrayOf(clickhouseDataTypeString, array.toArray)
    statement.setArray(i, jdbcArray)
  }

  override def clickhouseDataTypeString: String = s"Array(${typ.clickhouseDataTypeString})"

  override def convertInternalValue(value: Any): Seq[AnyRef] = typ.convertInternalArrayValue(value.asInstanceOf[ArrayData]).asInstanceOf[Seq[AnyRef]]
}