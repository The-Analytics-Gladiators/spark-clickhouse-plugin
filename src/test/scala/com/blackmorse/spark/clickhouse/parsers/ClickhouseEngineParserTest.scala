package com.blackmorse.spark.clickhouse.parsers

import com.blackmorse.spark.clickhouse.tables.services.TableParserService
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClickhouseEngineParserTest extends AnyFlatSpec with Matchers {
  "ClickhouseEngineParser" should "parse Distributed engine" in {
    val engineFull = "Distributed('cluster', 'default', 'table')"
    TableParserService.parseDistributedUnderlyingTable(engineFull) should be ("cluster", "default", "table")
  }
}
