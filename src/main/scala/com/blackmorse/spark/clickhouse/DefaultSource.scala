package com.blackmorse.spark.clickhouse

import com.blackmorse.spark.clickhouse.reader.ReaderClickhouseRelation
import com.clickhouse.client.ClickHouseDataType
import com.clickhouse.jdbc.ClickHouseDriver
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, TableScan}
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}

import java.sql.PreparedStatement
import java.util.Properties
import scala.collection.mutable
import scala.util.{Failure, Success, Using}

case class WriteClickhouseRelation(@transient sqlContext: SQLContext, schema: StructType) extends BaseRelation

class DefaultSource extends RelationProvider with CreatableRelationProvider {

  /**
   * Relation for writing into Clickhouse
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], dataFrame: DataFrame): BaseRelation = {
    val hostName = parameters(CLICKHOUSE_HOST_NAME)
    val port = parameters(CLICKHOUSE_PORT)
    val table = parameters(TABLE)
    val batchSize = parameters(BATCH_SIZE).toInt

    val url = s"jdbc:clickhouse://$hostName:$port"

    val schema = dataFrame.schema
    val columnsNumber = schema.size
    val values = Array.fill(columnsNumber)("?").mkString("(", ", ", ")")
    val fields = schema.map(_.name).mkString("(", ", ", ")")

    dataFrame
      .foreachPartition((iterator: scala.collection.Iterator[Row]) => {
        Using(new ClickHouseDriver().connect(url, new Properties())) {connection =>

          var rowsInBatch = 0
          var statement = connection.prepareStatement(s"INSERT INTO $table $fields VALUES $values")

          try {
            while (iterator.hasNext) {
              rowsInBatch += 1
              val row = iterator.next()

              schema.zipWithIndex.foreach { case (structField, i) =>
                structField.dataType match {
                  case BooleanType => statement.setBoolean(i + 1, row.getBoolean(i))
                  case ByteType => statement.setByte(i + 1, row.getByte(i))
                  case IntegerType =>
                    statement.setInt(i + 1, row.getInt(i))
                  case LongType => statement.setLong(i + 1, row.getLong(i))
                  case ShortType => statement.setShort(i + 1, row.getShort(i))
                  case FloatType => statement.setFloat(i + 1, row.getFloat(i))
                  case DoubleType => statement.setDouble(i + 1, row.getDouble(i))
                  case StringType => statement.setString(i + 1, row.getString(i))
                  case ArrayType(elementType, _containsNull) =>
                    val clickhouseTypeName = elementType match {
                      case BooleanType => ClickHouseDataType.UInt8.toString
                      case ByteType => ClickHouseDataType.Int8.toString
                      case IntegerType => ClickHouseDataType.Int32.toString
                      case LongType => ClickHouseDataType.Int64.toString
                      case ShortType => ClickHouseDataType.Int16.toString
                      case FloatType => ClickHouseDataType.Float32.toString
                      case DoubleType => ClickHouseDataType.Float64.toString
                      case StringType => ClickHouseDataType.String.toString
                    }
                    val array = row.getList(i).toArray
                    statement.setArray(i + 1, connection.createArrayOf(clickhouseTypeName, array))
                }
              }

              statement.addBatch()
              if (rowsInBatch >= batchSize) {
                statement.execute()
                statement = connection.prepareStatement(s"INSERT INTO $table $fields VALUES $values")
                rowsInBatch = 0
              }
            }
          } catch {
            case e: Throwable =>
              e.printStackTrace()
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

  /**
   * Relation for reading from Clickhouse
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val hostName = parameters(CLICKHOUSE_HOST_NAME)
    val port = parameters(CLICKHOUSE_PORT)
    val table = parameters(TABLE)

    val url = s"jdbc:clickhouse://$hostName:$port"
    val df = sqlContext.read
      .jdbc(url, table, new Properties())

    ReaderClickhouseRelation(sqlContext, parameters)
  }
}
