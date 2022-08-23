package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.blackmorse.spark.clickhouse.{CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE}
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}

import java.util.Properties
import scala.collection.mutable
import scala.util.{Failure, Success, Using}

case class ReaderClickhouseRelation(@transient sqlContext: SQLContext, parameters: Map[String, String])
  extends BaseRelation with TableScan with Serializable {

  private val hostName = parameters(CLICKHOUSE_HOST_NAME)
  private val port = parameters(CLICKHOUSE_PORT)
  private val table = parameters(TABLE)

  private val url = s"jdbc:clickhouse://$hostName:$port"
  private val clickhouseFields = ClickhouseSchemaParser.parseTable(url, table)

  override val schema: StructType =
    StructType(clickhouseFields.map(clickhouseField => StructField(clickhouseField.name, clickhouseField.typ.toSparkType(), clickhouseField.typ.nullable)))

  private val fieldsNames = clickhouseFields.map(field => s"`${field.name}`").mkString(",")


  override def buildScan(): RDD[Row] = {
    //TODO scale
    val clickhouseTimeZoneInfo = JDBCTimeZoneUtils.fetchClickhouseTimeZoneFromServer(url)

    sqlContext.sparkContext.parallelize(Seq(()), 1)
      .flatMap(_ => {
        val selectSQL = s"SELECT ${fieldsNames} FROM $table"
        Using(new ClickHouseDriver().connect(url, new Properties))(connection => {
          Using(connection.createStatement()) { stmt =>
            val rs = stmt.executeQuery(selectSQL)
            val rows = mutable.Buffer[Row]()
            while (rs.next()) {
              val fieldValues = clickhouseFields.map(_.extractFromRs(rs)(clickhouseTimeZoneInfo))
              rows += Row.fromSeq(fieldValues)
            }
            rows
          }
        })
      }.flatten match {
        case Success(rows) => rows
        case Failure(e) => throw e
      })
  }
}
