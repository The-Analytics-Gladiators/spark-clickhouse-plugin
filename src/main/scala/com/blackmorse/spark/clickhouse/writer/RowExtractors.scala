package com.blackmorse.spark.clickhouse.writer

import com.blackmorse.spark.clickhouse.reader.{ChType, LongCh}
import com.blackmorse.spark.clickhouse.sql.types.primitives.{ClickhouseBigIntType, ClickhouseDate, ClickhouseDate32, ClickhouseFloat32, ClickhouseFloat64, ClickhouseInt16, ClickhouseInt32, ClickhouseInt64, ClickhouseInt8, ClickhouseString, ClickhouseUInt16, ClickhouseUInt32, ClickhouseUInt64, ClickhouseUInt8}
import com.blackmorse.spark.clickhouse.sql.types.{ClickhouseArray, ClickhouseDateTime, ClickhouseDateTime64, ClickhouseDecimal, ClickhouseType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType

import java.sql.PreparedStatement

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

  trait RowWriter[T] {
    def writeFromRow(st: PreparedStatement, row: Row, index: Int) = write(st, row.get(index).asInstanceOf[T], index)
    def write(st: PreparedStatement, value: T, index: Int)
  }


  trait IntWriter extends RowWriter[Int] {
    override def write(st: PreparedStatement, value: Int, index: Int): Unit = st.setInt(index, value)
  }

  trait LongWriter extends RowWriter[Long] {
    override def write(st: PreparedStatement, value: Long, index: Int): Unit = st.setLong(index, value)
  }
  class DefaultIfNullWriter[T](delegate: RowWriter[T], default: T) extends RowWriter[T] {
    override def write(st: PreparedStatement, value: T, index: Int): Unit = {
      if(value == null) {
        delegate.write(st, default, index)
      } else {
        delegate.write(st, value, index)
      }
    }
  }

  class NullNotAllowedWriter[T](delegate: RowWriter[T]) extends RowWriter[T] {

    override def write(st: PreparedStatement, value: T, index: Int): Unit = {
      if(value == null) {
        throw new IllegalArgumentException("")
      } else {
        delegate.write(st, value, index)
      }
    }
  }

  trait RowElementExtractor[T] {
    def extract(row: Row, index: Int): T
  }

  trait IntRowReader extends RowElementExtractor[Int] {
    override def extract(row: Row, index: Int): Int = 2
  }

  trait LongRowReader extends RowElementExtractor[Long] {
    override def extract(row: Row, index: Int): Long = 3L
  }


  trait ReadWrite[T] extends RowElementExtractor[T] with RowWriter[T]

  // encode on type level
  def mapRowExtractors1[T >: ReadWrite[_]](sparkType: DataType, chType: ChType): T  =
    chType match {
      case LongCh(nullable) => new ReadWrite[Long] with LongRowReader with LongWriter
      case _ => throw new IllegalArgumentException()
    }

  case class RWService[T](read: RowElementExtractor[T], write: RowWriter[T])

  //encode with container class
  def mapRowExtractors2(sparkType: DataType, chType: ChType): RWService[_] =
    chType match {
      case LongCh(nullable) => if(nullable) {
        RWService[Long](read = new LongRowReader {}, write = new LongWriter {})
      } else {
        RWService[Long](read = new LongRowReader {}, write = new NullNotAllowedWriter[Long](new LongWriter {}))
      }
      case _ => throw new IllegalArgumentException()
    }
}
