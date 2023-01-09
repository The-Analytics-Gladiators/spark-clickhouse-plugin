package com.blackmorse.spark.clickhouse.utils

import com.blackmorse.spark.clickhouse.tables.ClickhouseTable

object SqlUtils {

  def prepareSqlQuery(
                       fields: String,
                       table: ClickhouseTable,
                       useForceCollapsingModifier: Boolean
                     ): String = {
    val sql = s"SELECT $fields FROM $table"
    table.engine match {
      case engine if useForceCollapsingModifier &&
        (engine.contains("CollapsingMergeTree") || engine.contains("ReplacingMergeTree")) => s"$sql FINAL"
      case _ => sql
    }
  }
}
