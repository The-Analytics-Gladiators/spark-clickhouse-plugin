package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
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


}
