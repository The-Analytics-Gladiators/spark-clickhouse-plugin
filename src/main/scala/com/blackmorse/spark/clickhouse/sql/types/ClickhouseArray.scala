package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType}

import java.sql.{PreparedStatement, ResultSet}

case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType {
  override type T = Seq[AnyRef]
  override lazy val defaultValue: T = Seq[AnyRef]()

  override def toSparkType(): DataType = ArrayType(typ.toSparkType(), typ.nullable)
  override val nullable: Boolean = false

  override def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    typ.extractArrayFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)

  override protected def setValueToStatement(i: Int, value: Seq[AnyRef], statement: PreparedStatement)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    val array = value.map{
      case null => typ.defaultValue.asInstanceOf[AnyRef]
      case el => el
    }
    val jdbcArray = statement.getConnection.createArrayOf(clickhouseDataTypeString, array.toArray)
    statement.setArray(i, jdbcArray)
  }

  override def clickhouseDataTypeString: String = s"Array(${typ.clickhouseDataTypeString})"
}

object ClickhouseArray {
  def mapRowExtractor(sparkType: DataType): (Row, Int) => Seq[Any] = (row, index) => sparkType match {
    case ArrayType(_, _) => row.getSeq[Any](index)
  }
}