package com.blackmorse.spark.clickhouse.spark.types

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseField
import com.clickhouse.jdbc.ClickHouseDriver

import java.util.Properties
import scala.collection.mutable
import scala.util.{Failure, Success, Using}

object ClickhouseSchemaParser {
  def parseTable(url: String, table: String, connectionProperties: Properties = new Properties()): Seq[ClickhouseField] = {
    Using(new ClickHouseDriver().connect(url, connectionProperties)) { connection =>
      Using(connection.createStatement().executeQuery(s"DESCRIBE TABLE $table")) { rs =>
        val buffer = mutable.Buffer[ClickhouseField]()
        while (rs.next()) {
          val name = rs.getString(1)
          val typ = rs.getString(2)
          buffer += ClickhouseField(name, ClickhouseTypesParser.parseType(typ))
        }
        buffer
      }
    }.flatten match {
      case Success(fields) => fields
      case Failure(exception) => throw exception
    }
  }


}
