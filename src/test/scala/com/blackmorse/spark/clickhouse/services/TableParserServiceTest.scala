package com.blackmorse.spark.clickhouse.services

import com.blackmorse.spark.clickhouse.tables.services.TableParserService.parseOrderingKey
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TableParserServiceTest extends AnyFlatSpec with Matchers {
  "parseOrderingKey" should "return None for no-ordering key" in {
    parseOrderingKey("ENGINE = TinyLog") should be (None)
  }

  "parse single key" should "return this key" in {
    parseOrderingKey("ENGINE = MergeTree() ORDER BY field") should be (Some("field"))
  }

  "parse double key" should "return string" in {
    parseOrderingKey("ENGINE = MergeTree() ORDER BY (field1, field2) OTHER STUFF") should be (Some("field1, field2"))
  }

  "parse key with brackets" should "work" in {
    parseOrderingKey("ENGINE = MergeTree ORDER BY (field1, cityHash64(field_2))") should be (Some("field1, cityHash64(field_2)"))
  }

  "parse 'tuple()' ordering key" should "be parsed to None" in {
    parseOrderingKey("ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8196") should be (None)
  }

  //TODO order by single key with spaces. E.g. "ORDER BY someFunc(a, b)"
}
