package com.blackmorse.spark.clickhouse.reader

import com.blackmorse.spark.clickhouse.utils.JDBCTimeZoneUtils
import com.blackmorse.spark.clickhouse.{CLICKHOUSE_HOST_NAME, CLICKHOUSE_PORT, TABLE}
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.ClickhouseRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import java.util.Properties

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
    new ClickhouseRdd[Row](sqlContext.sparkContext,
      () => new ClickHouseDriver().connect(url, new Properties()),
      s"SELECT ${fieldsNames} FROM $table",
      rs => {
        val fieldValues = clickhouseFields.map(_.extractFromRs(rs)(clickhouseTimeZoneInfo))
        Row.fromSeq(fieldValues)
      })
  }
}



