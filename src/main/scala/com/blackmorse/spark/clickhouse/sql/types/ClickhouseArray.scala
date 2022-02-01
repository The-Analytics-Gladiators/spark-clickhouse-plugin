package com.blackmorse.spark.clickhouse.sql.types

import com.clickhouse.client.ClickHouseDataType
import org.apache.spark.sql.types.{ArrayType, DataType}

import java.math.BigInteger
import java.sql.{ResultSet, Timestamp}
import java.time.LocalDateTime

case class ClickhouseArray(typ: ClickhouseType) extends ClickhouseType {
  override def toSparkType(): DataType = ArrayType(typ.toSparkType(), typ.nullable)
  override val nullable: Boolean = false

  override def extractFromRs(name: String, resultSet: ResultSet): Any = {
    typ match {
        //TODO
      case PrimitiveClickhouseType(ClickHouseDataType.Int128 | ClickHouseDataType.UInt128 | ClickHouseDataType.Int256 | ClickHouseDataType.UInt256, _, _)  =>
        resultSet.getArray(name).getArray.asInstanceOf[Array[BigInteger]].map(bi => new java.math.BigDecimal(bi))
      case ClickhouseDateTime(_, _) | ClickhouseDateTime64(_, _) =>
        resultSet.getArray(name).getArray.asInstanceOf[Array[LocalDateTime]].map(ldt => Timestamp.valueOf(ldt))
      case _ =>
        resultSet.getArray(name).getArray
    }
  }
}
