package com.blackmorse.spark.clickhouse.utils

import com.clickhouse.jdbc.ClickHouseDriver

import java.sql.ResultSet
import java.util.Properties
import scala.collection.mutable
import scala.util.{Try, Using}

object JDBCUtils {
  def executeSql[T](url: String, connectionProperties: Properties = new Properties())(sql: String)(mapper: ResultSet => T): Try[Seq[T]] =
    Using(new ClickHouseDriver().connect(url, connectionProperties)) { connection =>
      Using(connection.createStatement().executeQuery(sql)) { rs =>
        val buffer = mutable.Buffer[T]()
        while (rs.next()) {
          buffer += mapper(rs)
        }
        buffer.toSeq
      }
    }.flatten
}
