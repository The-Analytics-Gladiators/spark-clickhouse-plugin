package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.{CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

import java.util.Properties

case class ReaderClickhouseRelation(@transient sqlContext: SQLContext, parameters: Map[String, String])
  extends BaseRelation with TableScan with Serializable {

  val hostName = parameters(CLICKHOUSE_HOST_NAME)
  val port = parameters(CLICKHOUSE_PORT)
  val table = parameters(TABLE)

  val url = s"jdbc:clickhouse://$hostName:$port"
  val df = sqlContext.read
    .jdbc(url, table, new Properties())

  override def schema: StructType = df.schema

  override def buildScan(): RDD[Row] = df.rdd
}
