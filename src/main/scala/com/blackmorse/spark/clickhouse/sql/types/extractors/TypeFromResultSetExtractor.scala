package com.blackmorse.spark.clickhouse.sql.types.extractors

import com.blackmorse.spark.clickhouse.sql.types.ClickhouseArray
import com.blackmorse.spark.clickhouse.utils.{ClickhouseTimeZoneInfo, JDBCTimeZoneUtils}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

import java.sql.ResultSet

trait TypeFromResultSetExtractor {
  final def extractFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    if (resultSet.getObject(name) == null) {
      null
    } else {
      extractNonNullableFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)
    }
  }

  def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any
}

trait StringRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    UTF8String.fromString(resultSet.getString(name))
}

trait BooleanRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getBoolean(name)
}

trait DateRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    val date = resultSet.getDate(name).toLocalDate
    val millis = JDBCTimeZoneUtils.localDateToDate(date, clickhouseTimeZoneInfo)
      .getTime

    (millis / 1000 / 60 / 60 / 24).toInt
  }
}

trait FloatRSEXtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getFloat(name)
}

trait DoubleRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getDouble(name)
}

trait ByteRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getByte(name)
}

trait ShortRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getShort(name)
}

trait IntRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getInt(name)
}

trait LongRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getLong(name)
}

trait DecimalRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    val bd = resultSet.getBigDecimal(name).setScale(0)
    Decimal(bd)
  }
}

trait ArrayRSExtractor extends TypeFromResultSetExtractor { self: ClickhouseArray =>
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any = {
    val array = typ.extractArrayFromRsByName(name, resultSet)(clickhouseTimeZoneInfo)
    ArrayData.toArrayData(array)
  }
}

trait TimestampRSExtractor extends TypeFromResultSetExtractor {
  override def extractNonNullableFromRsByName(name: String, resultSet: ResultSet)(clickhouseTimeZoneInfo: ClickhouseTimeZoneInfo): Any =
    resultSet.getTimestamp(name).getTime * 1000
}
