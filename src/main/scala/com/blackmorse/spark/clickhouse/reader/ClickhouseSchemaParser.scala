package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.sql.types.{ClickhouseField, ClickhouseField1}
import com.clickhouse.jdbc.ClickHouseDriver

import java.util.Properties
import scala.collection.mutable
import scala.util.{Failure, Success, Using}

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


  def parseTable1(url: String, table: String): Seq[ClickhouseField1] = {
    Using(new ClickHouseDriver().connect(url, new Properties())) { connection =>
      val rs = connection.createStatement().executeQuery(s"DESCRIBE TABLE $table")
      val buffer = mutable.Buffer[ClickhouseField1]()
      while (rs.next()) {
        val name = rs.getString(1)
        val typ = rs.getString(2)
        buffer += ClickhouseField1(name, ClickhouseTypesParser.parseType1(typ))
      }
      buffer
    } match {
      case Success(fields) => fields
      case Failure(exception) => throw exception
    }
  }
}

case class ArrayCh(inner: ChType, nullable: Boolean) extends ChType
case class LongCh(nullable: Boolean) extends ChType
case class IntCh(nullable: Boolean) extends ChType
trait ChType {
  def nullable: Boolean
}
