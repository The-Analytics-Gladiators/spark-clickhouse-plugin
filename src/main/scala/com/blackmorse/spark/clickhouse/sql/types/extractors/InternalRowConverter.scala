package com.blackmorse.spark.clickhouse.sql.types.extractors

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseArray
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{Date, Timestamp}

trait InternalRowValueConverter[T] {
  def convertInternalValue(value: Any): T
}

trait StandardInternalRowConverter[T] extends InternalRowValueConverter[T] {
  override def convertInternalValue(value: Any): T = value.asInstanceOf[T]
}

trait StringInternalRowConverter extends InternalRowValueConverter[String] {
  override def convertInternalValue(value: Any): String =
    value.asInstanceOf[UTF8String].toString
}

trait DateIntervalRowConverter extends InternalRowValueConverter[Date] {
  override def convertInternalValue(value: Any): Date =
    new Date(value.asInstanceOf[Integer].toLong * 1000 * 60 * 60 * 24)
}

trait DateTimeInternalRowConverter extends InternalRowValueConverter[Timestamp] {
  override def convertInternalValue(value: Any): Timestamp =
    new Timestamp(value.asInstanceOf[Long] / 1000)
}

trait DecimalInternalRowConverter extends InternalRowValueConverter[java.math.BigDecimal] {
  override def convertInternalValue(value: Any): java.math.BigDecimal =
    value.asInstanceOf[Decimal].toJavaBigDecimal
}

trait ArrayInternalRowConverter extends InternalRowValueConverter[Seq[AnyRef]] { self: ClickhouseArray =>
  override def convertInternalValue(value: Any): Seq[AnyRef] =
    typ.convertInternalArrayValue(value.asInstanceOf[ArrayData], self.typ.toSparkType).asInstanceOf[Seq[AnyRef]]
}
