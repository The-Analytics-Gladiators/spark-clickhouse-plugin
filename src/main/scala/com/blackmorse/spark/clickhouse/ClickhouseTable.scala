package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.reader.{ClickhouseReaderConfiguration, ClickhouseScanBuilder}
import com.blackmorse.spark.clickhouse.writer.ClickhouseWriterBuilder
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class ClickhouseTable(clickhouseReaderInfo: ClickhouseReaderConfiguration)
    extends Table
    with SupportsRead
    with SupportsWrite {
  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new ClickhouseScanBuilder(clickhouseReaderInfo)

  override def name(): String = clickhouseReaderInfo.tableName

  override def schema(): StructType = clickhouseReaderInfo.schema

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new ClickhouseWriterBuilder(info)
}

