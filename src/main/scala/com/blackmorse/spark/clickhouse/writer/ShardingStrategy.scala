package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.{RANDOM_WRITES_SHUFFLE, SHARD_FIELD}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

sealed trait ShardingStrategy extends Serializable

object ShardingStrategy {
  def parseStrategy(options: CaseInsensitiveStringMap): ShardingStrategy = {
    if (!options.containsKey(RANDOM_WRITES_SHUFFLE) && !options.containsKey(SHARD_FIELD)) {
      SparkPartition
    } else if (options.containsKey(SHARD_FIELD)){
      ShardByField(options.get(SHARD_FIELD))
    } else if (options.containsKey(RANDOM_WRITES_SHUFFLE)) {
      RandomShuffle
    } else {
      throw new IllegalArgumentException("Wrong sharding settings. Probably you've specified both randomShuffle and field's sharding")
    }
  }
}

/**
 * Whole Spark partition will be written into one shard
 */
object SparkPartition extends ShardingStrategy

object RandomShuffle extends ShardingStrategy

case class ShardByField(field: String) extends ShardingStrategy
