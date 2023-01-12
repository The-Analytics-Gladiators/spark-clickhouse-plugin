package com.blackmorse.spark.clickhouse.utils

import com.blackmorse.spark.clickhouse.CH_PROPERTIES_PREFIX
import ru.yandex.clickhouse.settings.{ClickHouseConnectionSettings, ClickHouseQueryParam}


object PropertiesUtils {
  def httpParams(map: Map[String, String]): String = {
    val optionsFromChProperties = map
      .flatMap { case (key, value) =>
        if (ClickHouseConnectionSettings.values().exists(v => v.getKey == key.toLowerCase)
          || ClickHouseQueryParam.values().exists(v => v.getKey == key.toLowerCase)) {
          Some(s"${key.toLowerCase}=$value")
        } else {
          None
        }
      }

      val optionsWithPrefix = map
        .filter{case(key, _) => key.startsWith(CH_PROPERTIES_PREFIX)}
        .map{case(key, value) => (key.substring(CH_PROPERTIES_PREFIX.length), value)}
        .map{case(key, value) => s"${key.toLowerCase}=$value"}
        .toSeq

      (optionsFromChProperties.toSeq ++ optionsWithPrefix)
        .sorted
        .mkString(",")
  }
}
