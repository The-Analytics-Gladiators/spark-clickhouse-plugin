package com.blackmorse.spark.clickhouse.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PropertiesUtilsTests extends AnyFlatSpec with Matchers {
  "Properties from ClickhouseConnectionSettings" should "be passed" in {
    val map = Map("socket_timeout" -> "1234")
    val options = PropertiesUtils.httpParams(map)

    options shouldBe ("socket_timeout=1234")
  }

  "Properties from ClickhouseQueryParams" should "be passed" in {
    val map = Map("session_timeout" -> "9000")
    val options = PropertiesUtils.httpParams(map)

    options shouldBe ("session_timeout=9000")
  }

  "Properties with proper prefix" should "be passed" in {
    val map = Map("http_params_insert_quorum" -> "2")
    val options = PropertiesUtils.httpParams(map)

    options shouldBe ("insert_quorum=2")
  }

  "All three types of properties" should "be concatenated with comma" in {
    val map = Map("read_backoff_min_latency_ms" -> "2000",
      "max_redirects" -> "10",
      "http_params_max_memory_usage" -> "1000000")

    val options = PropertiesUtils.httpParams(map)

    options shouldBe ("max_memory_usage=1000000,max_redirects=10,read_backoff_min_latency_ms=2000")
  }
}
