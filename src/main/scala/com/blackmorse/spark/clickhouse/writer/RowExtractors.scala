package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseBigIntType, ClickhouseDate, ClickhouseDate32, ClickhouseFloat32, ClickhouseFloat64, ClickhouseInt16, ClickhouseInt32, ClickhouseInt64, ClickhouseInt8, ClickhouseString, ClickhouseUInt16, ClickhouseUInt32, ClickhouseUInt64, ClickhouseUInt8}
import com.blackmorse.spark.clickhouse.sql.types.{ClickhouseArray, ClickhouseDateTime, ClickhouseDateTime64, ClickhouseDecimal, ClickhouseType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

object RowExtractors {
  def mapRowExtractors(sparkType: DataType, chType: ClickhouseType): (Row, Int) => Any =
    chType match {
      case ClickhouseDate(_, _) => ClickhouseDate.mapRowExtractor(sparkType)
      case ClickhouseDate32(_, _) => ClickhouseDate32.mapRowExtractor(sparkType)
      case ClickhouseFloat32(_, _) => ClickhouseFloat32.mapRowExtractor(sparkType)
      case ClickhouseFloat64(_, _) => ClickhouseFloat64.mapRowExtractor(sparkType)
      case ClickhouseInt8(_, _) => ClickhouseInt8.mapRowExtractor(sparkType)
      case ClickhouseInt16(_, _) => ClickhouseInt16.mapRowExtractor(sparkType)
      case ClickhouseInt32(_, _) => ClickhouseInt32.mapRowExtractor(sparkType)
      case ClickhouseInt64(_, _) => ClickhouseInt64.mapRowExtractor(sparkType)
      case ClickhouseString(_, _) => ClickhouseString.mapRowExtractor(sparkType)
      case ClickhouseUInt8(_, _) => ClickhouseUInt8.mapRowExtractor(sparkType)
      case ClickhouseUInt16(_, _) => ClickhouseUInt16.mapRowExtractor(sparkType)
      case ClickhouseUInt32(_, _) => ClickhouseUInt32.mapRowExtractor(sparkType)
      case ClickhouseUInt64(_, _) => ClickhouseUInt64.mapRowExtractor(sparkType)
      case ClickhouseDateTime(_, _) => ClickhouseDateTime.mapRowExtractor(sparkType)
      case ClickhouseDateTime64(_, _) => ClickhouseDateTime64.mapRowExtractor(sparkType)
      case ClickhouseDecimal(_, _, _) => ClickhouseDecimal.mapRowExtractor(sparkType)
      case _: ClickhouseBigIntType => ClickhouseBigIntType.mapRowExtractor(sparkType)
      case ClickhouseArray(_)      => ClickhouseArray.mapRowExtractor(sparkType)
  }
}
