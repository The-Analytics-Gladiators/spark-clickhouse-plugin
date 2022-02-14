package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.reader.ClickhouseSchemaParser
import com.blackmorse.spark.clickhouse.spark.types.{SchemaMerger, SparkTypeMapper}
import com.blackmorse.spark.clickhouse.{BATCH_SIZE, CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE, WriteClickhouseRelation}
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import java.util.Properties
import scala.util.{Failure, Success, Using}

object ClickhouseWriter {
  def writeDataFrame(dataFrame: DataFrame, parameters: Map[String, String], sqlContext: SQLContext): BaseRelation = {
    val hostName = parameters(CLICKHOUSE_HOST_NAME)
    val port = parameters(CLICKHOUSE_PORT)
    val table = parameters(TABLE)
    val batchSize = parameters(BATCH_SIZE).toInt

    val url = s"jdbc:clickhouse://$hostName:$port"

    val schema = dataFrame.schema
    val columnsNumber = schema.size
    val values = Array.fill(columnsNumber)("?").mkString("(", ", ", ")")
    val fields = schema.map(_.name).mkString("(", ", ", ")")

    val clickhouseFields = ClickhouseSchemaParser.parseTable(url, table)

    val mergedSchema = SchemaMerger.mergeSchemas(schema, clickhouseFields)

    dataFrame
      .foreachPartition((iterator: scala.collection.Iterator[Row]) => {
        Using(new ClickHouseDriver().connect(url, new Properties())) {connection =>

          val rs = connection.createStatement().executeQuery("SELECT timeZone()")
          rs.next()
          val clickhouseTimeZone = rs.getString(1)
          val clickhouseTimeZoneInfo = ClickhouseTimeZoneInfo(clickhouseTimeZone)

          var rowsInBatch = 0
          var statement = connection.prepareStatement(s"INSERT INTO $table $fields VALUES $values")

          while (iterator.hasNext) {
            rowsInBatch += 1
            val row = iterator.next()

            mergedSchema.zipWithIndex.foreach { case ((sparkField, chField), index) =>
              SparkTypeMapper
                .mapType(sparkField.dataType, chField.typ)
                .extractFromRowAndSetToStatement(index, row, statement)(clickhouseTimeZoneInfo)
            }

            statement.addBatch()
            if (rowsInBatch >= batchSize) {
              statement.execute()
              statement = connection.prepareStatement(s"INSERT INTO $table $fields VALUES $values")
              rowsInBatch = 0
            }
          }

          if (rowsInBatch > 0) {
            statement.executeBatch()
          }
        } match {
          case Success(_) =>
          case Failure(exception) => throw exception
        }
      })

    WriteClickhouseRelation(sqlContext, schema)
  }
}
