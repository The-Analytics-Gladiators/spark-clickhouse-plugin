package com.blackmorse.spark.clickhouse.types

import com.blackmorse.spark.clickhouse.sql.types.primitives.ClickhouseBoolean
import com.blackmorse.spark.clickhouse.types.BaseTestCases.{testPrimitive, testPrimitiveAndArray}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}
import org.scalatest.flatspec.AnyFlatSpec

class BoolTests extends AnyFlatSpec with DataFrameSuiteBase {
  import sqlContext.implicits._


  "Bool" should "be supported" in {
    //TODO Impossible to set Array[AnyRef] with Booleans for JDBC driver
    testPrimitive(clickhouseType = ClickhouseBoolean(nullable = false, lowCardinality = false))(
      Seq(true, false),
      row => row.getBoolean(0),
      ClickhouseBoolean(false, false).toSparkType,
      convertToOriginalType = _.asInstanceOf[Boolean],
      comparator = (b,s ) => b == s
    )
  }

  //Should decide: do we want to auto-convert numeric types into boolean?
  "Bool" should "work with ByteType" ignore {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        (0 to 100) map (_.toByte),
        Seq(Byte.MinValue, Byte.MaxValue)
      ),
      forceSparkType = ByteType,
      convertToOriginalType = b => if (b.asInstanceOf[Byte] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }

  "Bool" should "work with ShortType" ignore {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        (-100 to 100) map (_.toShort),
        Seq(Short.MinValue, Short.MaxValue)
      ),
      forceSparkType = ShortType,
      convertToOriginalType = b => if (b.asInstanceOf[Short] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }

  "Bool" should "work with IntegerType" ignore {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        -100 to 100,
        Seq(Int.MinValue, Int.MaxValue)
      ),
      forceSparkType = IntegerType,
      convertToOriginalType = b => if (b.asInstanceOf[Int] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }

  "Bool" should "work with LongType" ignore {
    testPrimitiveAndArray(ClickhouseBoolean(nullable = false, lowCardinality = false))(
      cases = Seq(
        (-100 to 100) map (_.toLong),
        Seq(Long.MinValue, Long.MaxValue)
      ),
      forceSparkType = LongType,
      convertToOriginalType = b => if (b.asInstanceOf[Long] == 0) false else true,
      rowConverter = row => row.getBoolean(0)
    )
  }
}
