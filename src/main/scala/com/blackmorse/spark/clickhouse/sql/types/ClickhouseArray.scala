package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType}

import java.sql.{PreparedStatement, ResultSet, Timestamp}

case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType {
  override def toSparkType(): DataType = ArrayType(typ.toSparkType(), typ.nullable)
  override val nullable: Boolean = false

  override def extractFromRs(name: String, resultSet: ResultSet): Any =
    typ.extractArray(name, resultSet)

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit = {
    val array = typ match {
        //Set timezone to array
      case ClickhouseDateTime(_, _) | ClickhouseDateTime64(_, _) =>
        row.getList(i).toArray.map(el => ((el.asInstanceOf[Timestamp].getTime + clickhouseTimeZoneInfo.timeZoneMillisDiff) / 1000).asInstanceOf[Object])
      case _ => row.getList(i).toArray
    }
    val jdbcArray = statement.getConnection.createArrayOf(typ.arrayClickhouseTypeString(), array)
    statement.setArray(i + 1, jdbcArray)
  }

  override def arrayClickhouseTypeString(): String = s"Array(${typ.arrayClickhouseTypeString()})"
}
