package com.blackmorse.spark.clickhouse.reader

import com.clickhouse.client.ClickHouseDataType
import com.clickhouse.jdbc.ClickHouseDriver

import java.util.Properties
import scala.collection.mutable
import scala.util.{Failure, Success, Using}

sealed trait ClickhouseType

case class PrimitiveClickhouseType(typ: ClickHouseDataType, nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType
case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType
case class ClickhouseMap(key: ClickhouseType, value: ClickhouseType, nullable: Boolean) extends ClickhouseType

case class ClickhouseField(name: String, typ: ClickhouseType)

object ClickhouseSchemaParser {
  def parseTable(url: String, table: String): Seq[ClickhouseField] = {
    Using(new ClickHouseDriver().connect(url, new Properties())) { connection =>
      val rs = connection.createStatement().executeQuery(s"DESCRIBE TABLE $table")
      val buffer = mutable.Buffer[ClickhouseField]()
      while (rs.next()) {
        val name = rs.getString(1)
        val typ = rs.getString(2)
        buffer += ClickhouseField(name, ClickhouseTypesParser.parseType(typ))
      }
      buffer
    } match {
      case Success(fields) => fields
      case Failure(exception) => throw exception
    }
  }


}
