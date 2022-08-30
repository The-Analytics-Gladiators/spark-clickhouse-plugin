package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType {
  override type T = Array[AnyRef]
  override lazy val defaultValue: T = Array[AnyRef]()

  override def toSparkType(): DataType = ArrayType(typ.toSparkType(), typ.nullable)
  override val nullable: Boolean = false

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    typ.extractArrayFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)

  override def extractFromRow(i: Int, row: Row): Array[AnyRef] =
    row.getList[AnyRef](i).toArray


  override protected def setValueToStatement(i: Int, value: Array[AnyRef], statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    val array = value.map{
      case null => typ.defaultValue.asInstanceOf[AnyRef]
      case el => el
    }
    val jdbcArray = statement.getConnection.createArrayOf(clickhouseDataTypeString, array)
    statement.setArray(i, jdbcArray)
  }

  override def clickhouseDataTypeString: String = s"Array(${typ.clickhouseDataTypeString})"
}
