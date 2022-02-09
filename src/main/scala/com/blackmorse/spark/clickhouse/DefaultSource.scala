package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.reader.ReaderClickhouseRelation
import com.blackmorse.spark.clickhouse.writer.ClickhouseWriter
import com.clickhouse.client.ClickHouseDataType
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, TableScan}
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampType}

import java.sql.Timestamp
import java.util.Calendar
import java.util.{Calendar, Properties, TimeZone}
import scala.util.{Failure, Success, Using}

case class WriteClickhouseRelation(@transient sqlContext: SQLContext, schema: StructType) extends BaseRelation

class DefaultSource extends RelationProvider with CreatableRelationProvider {

  /**
   * Relation for writing into Clickhouse
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], dataFrame: DataFrame): BaseRelation = {

    ClickhouseWriter.writeDataFrame(dataFrame, parameters, sqlContext)
  }

  /**
   * Relation for reading from Clickhouse
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    ReaderClickhouseRelation(sqlContext, parameters)
  }
}
