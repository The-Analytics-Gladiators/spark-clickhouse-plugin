package com.blackmorse.spark.clickhouse.services

import com.blackmorse.spark.clickhouse.services.ClickhouseDistributedTableService.parseOrderingKey
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClickhouseDistributedTableServiceTest extends AnyFlatSpec with Matchers {
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

  //TODO order by single key with spaces. E.g. "ORDER BY someFunc(a, b)"
}
