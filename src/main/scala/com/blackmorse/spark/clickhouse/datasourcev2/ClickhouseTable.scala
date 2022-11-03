package com.blackmorse.spark.clickhouse.datasourcev2

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class ClickhouseTable(clickhouseReaderInfo: ClickhouseReaderInfo) extends Table with SupportsRead {
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ClickhouseScanBuilder(clickhouseReaderInfo)

  override def name(): String = clickhouseReaderInfo.tableName

  override def schema(): StructType = clickhouseReaderInfo.schema
}

