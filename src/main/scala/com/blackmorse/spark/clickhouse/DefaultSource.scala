package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.reader.ReaderClickhouseRelation
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider}
import org.apache.spark.sql.types.StructType

import java.util.Properties

case class WriteClickhouseRelation(@transient sqlContext: SQLContext, schema: StructType) extends BaseRelation

class DefaultSource extends RelationProvider with CreatableRelationProvider {

  /**
   * Relation for writing into Clickhouse
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], dataFrame: DataFrame): BaseRelation = {
    val hostName = parameters(CLICKHOUSE_HOST_NAME)
    val port = parameters(CLICKHOUSE_PORT)
    val table = parameters(TABLE)

    val url = s"jdbc:clickhouse://$hostName:$port"

    dataFrame.write
      .mode(SaveMode.Append)
      .jdbc(url, table, new Properties())

    WriteClickhouseRelation(sqlContext, dataFrame.schema)
  }

  /**
   * Relation for reading from Clickhouse
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    ReaderClickhouseRelation(sqlContext, parameters)
  }
}
