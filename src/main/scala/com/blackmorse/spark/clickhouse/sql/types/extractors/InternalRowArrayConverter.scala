package com.blackmorse.spark.clickhouse.sql.types.extractors

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType}
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Timestamp

trait InternalRowArrayConverter[T] {
  def convertInternalArrayValue(value: ArrayData, sparkType: DataType): Seq[T]
}

trait StandardRowArrayConverter[T] extends InternalRowArrayConverter[T] {
  override def convertInternalArrayValue(value: ArrayData, sparkType: DataType): Seq[T] =
    value.toSeq(sparkType)
}

trait StringRowArrayConverter extends InternalRowArrayConverter[String] {
  override def convertInternalArrayValue(value: ArrayData, sparkType: DataType): Seq[String] =
    value.toSeq[UTF8String](sparkType)
      .map(s => if(s== null) null else s.toString)
}

trait DateTimeRowArrayConverter extends InternalRowArrayConverter[Timestamp] {
  override def convertInternalArrayValue(value: ArrayData, sparkType: DataType): Seq[Timestamp] =
    value.toSeq[Long](sparkType).map(l => new Timestamp(l / 1000))
}

trait UInt64InternalRowConverter extends InternalRowArrayConverter[java.math.BigDecimal] {
  def convertInternalArrayValue(value: ArrayData, sparkType: DataType): Seq[java.math.BigDecimal] = {
    value.toSeq[Decimal](DecimalType(38, 0))
      .map(value => if (value == null) null else value.toJavaBigDecimal)
  }
}
