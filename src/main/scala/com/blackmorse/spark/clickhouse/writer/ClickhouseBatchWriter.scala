package com.blackmorse.spark.clickhouse.writer

import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import ru.yandex.clickhouse.settings.ClickHouseProperties

import java.util.Properties
import scala.collection.mutable
import scala.util.Using

class ClickhouseBatchWriter(clickHouseDriver: ClickHouseDriver,
                            url: String,
                            connectionProps: Properties,
                            sql: String,
                            batchSize: Int) extends Logging {
  //TODO unify URLs
  private val jdbcUrl = if(url.startsWith("jdbc")) url else s"jdbc:clickhouse://$url"
  private val clickhouseProperties = new ClickHouseProperties()
  private val connection = clickHouseDriver.connect(jdbcUrl, connectionProps)
  private var statement = connection.prepareStatement(sql)
  private var rowsInBatch = 0

  def addRow(record: InternalRow, fields: Seq[Field]): Unit = {
    rowsInBatch += 1
    fields.foreach(field => {
      val rowIsNull = record.isNullAt(field.index)
      val statementIndex = field.index + 1

      if (rowIsNull && field.chType.nullable) {
        statement.setObject(statementIndex, null)
      } else if (rowIsNull) {
        field.chType.setValueToStatement(statementIndex, field.chType.defaultValue, statement)(field.clickhouseTimeZoneInfo)
      } else {
        val extracted = InternalRow.getAccessor(field.sparkDataType, field.chType.nullable)(record, field.index)
        field.chType.setValueToStatement(statementIndex, field.chType.convertInternalValue(extracted), statement)(field.clickhouseTimeZoneInfo)
      }
    })

    statement.addBatch()
    if (rowsInBatch >= batchSize) {
      log.debug(s"Batch complete, sending $rowsInBatch records")
      Using(statement)(stmt => stmt.executeBatch())
      statement = connection.prepareStatement(sql)
      rowsInBatch = 0

    }
  }

  def flush(): Unit = {
    statement.executeBatch()
  }

  def close(): Unit = {
    try {
      if (null != statement) {
        statement.close()
      }
    } catch {
      case exception: Exception => logWarning("Exception closing statement", exception)
    }
    try {
      if (null != connection) {
        connection.close()
      }
      logDebug("closed connection")
    } catch {
      case exception: Exception => logWarning("Exception closing connection", exception)
    }
  }
}
