package com.blackmorse.spark.clickhouse.utils

import com.blackmorse.spark.clickhouse.tables.ClickhouseTable

object SqlUtils {

  def prepareSqlQuery(
                       fields: String,
                       table: ClickhouseTable,
                       useForceCollapsingModifier: Boolean
                     ): String = {
    val sql = s"SELECT $fields FROM $table"
    if (useForceCollapsingModifier &&
      (table.engine.contains("CollapsingMergeTree") || table.engine.contains("ReplacingMergeTree")))
      s"$sql FINAL"
    else sql
  }
}
