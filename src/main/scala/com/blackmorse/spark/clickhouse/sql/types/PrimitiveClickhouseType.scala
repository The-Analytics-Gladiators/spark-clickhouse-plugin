package com.blackmorse.spark.clickhouse.sql.types

import com.blackmorse.spark.clickhouse.writer.ClickhouseTimeZoneInfo
import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import java.sql.{PreparedStatement, ResultSet}

case class PrimitiveClickhouseType(typ: ClickHouseDataType, nullable: Boolean, lowCardinality: Boolean) extends ClickhouseType {
  override def toSparkType(): DataType = {
    typ match {
      case ClickHouseDataType.Int8 => ByteType
      case ClickHouseDataType.UInt8 => ShortType
      case ClickHouseDataType.Int16 => ShortType
      case ClickHouseDataType.UInt16 => IntegerType
      case ClickHouseDataType.Int32 => IntegerType
      case ClickHouseDataType.UInt32 => LongType
      case ClickHouseDataType.Int64 => LongType
      case ClickHouseDataType.UInt64 => DecimalType(38, 0)
      case ClickHouseDataType.Int128 => DecimalType(38, 0)
      case ClickHouseDataType.UInt128 => DecimalType(38, 0)
      case ClickHouseDataType.Int256 => DecimalType(38, 0)
      case ClickHouseDataType.UInt256 => DecimalType(38, 0)

      case ClickHouseDataType.String => StringType
      case ClickHouseDataType.Float32 => FloatType
      case ClickHouseDataType.Float64 => DoubleType
      case ClickHouseDataType.Date => DateType
      case ClickHouseDataType.Date32 => DateType
    }
  }

  override def extractFromRs(name: String, resultSet: ResultSet): Any =
    typ match {
      case ClickHouseDataType.Int8 => resultSet.getByte(name)
      case ClickHouseDataType.UInt8 => resultSet.getShort(name)
      case ClickHouseDataType.Int16 => resultSet.getShort(name)
      case ClickHouseDataType.UInt16 => resultSet.getInt(name)
      case ClickHouseDataType.Int32 => resultSet.getInt(name)
      case ClickHouseDataType.UInt32 => resultSet.getLong(name)
      case ClickHouseDataType.Int64 => resultSet.getLong(name)
      case ClickHouseDataType.UInt64 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.Int128 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.UInt128 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.Int256 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.UInt256 => resultSet.getBigDecimal(name)
      case ClickHouseDataType.Date => resultSet.getDate(name)
      case ClickHouseDataType.Date32 => resultSet.getDate(name)

      case ClickHouseDataType.String => resultSet.getString(name)
      case ClickHouseDataType.Float32 => resultSet.getFloat(name)
      case ClickHouseDataType.Float64 => resultSet.getDouble(name)
    }

  override def extractFromRowAndSetToStatement(i: Int, row: Row, statement: PreparedStatement)
                                              (clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Unit =
    typ match {
      case ClickHouseDataType.Int8 => statement.setByte(i + 1, row.getByte(i))
      case ClickHouseDataType.UInt8 => statement.setShort(i + 1, row.getShort(i))
      case ClickHouseDataType.Int16 => statement.setShort(i + 1, row.getShort(i))
      case ClickHouseDataType.UInt16 => statement.setInt(i + 1, row.getInt(i))
      case ClickHouseDataType.Int32 => statement.setInt(i + 1, row.getInt(i))
      case ClickHouseDataType.UInt32 => statement.setLong(i + 1, row.getLong(i))
      case ClickHouseDataType.Int64 => statement.setLong(i + 1, row.getLong(i))
      case ClickHouseDataType.UInt64 => statement.setBigDecimal(i + 1, row.getDecimal(i))
      case ClickHouseDataType.Int128 => statement.setBigDecimal(i + 1, row.getDecimal(i))
      case ClickHouseDataType.UInt128 => statement.setBigDecimal(i + 1, row.getDecimal(i))
      case ClickHouseDataType.Int256 => statement.setBigDecimal(i + 1, row.getDecimal(i))
      case ClickHouseDataType.UInt256 => statement.setBigDecimal(i + 1, row.getDecimal(i))
      case ClickHouseDataType.Date => statement.setDate(i + 1, row.getDate(i))
      case ClickHouseDataType.Date32 => statement.setDate(i + 1, row.getDate(i))

      case ClickHouseDataType.String => statement.setString(i + 1, row.getString(i))
      case ClickHouseDataType.Float32 => statement.setFloat(i + 1, row.getFloat(i))
      case ClickHouseDataType.Float64 => statement.setDouble(i + 1, row.getDouble(i))
    }

  override def arrayClickhouseTypeString(): String = s"Array(${typ.toString})"
}
