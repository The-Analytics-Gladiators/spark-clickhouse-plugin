package com.blackmorse.spark.clickhouse.services

import com.blackmorse.spark.clickhouse.ClickhouseHosts.{clusterName, shard1Replica1}
import com.blackmorse.spark.clickhouse.tables.{DistributedTable, GenericTable, MergeTreeTable}
import com.blackmorse.spark.clickhouse.tables.services.TableInfoService
import com.blackmorse.spark.clickhouse.utils.JDBCUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Properties

class TableInfoServiceTest extends AnyFlatSpec with Matchers {
  private val url = s"jdbc:clickhouse://$shard1Replica1"

  "TableInfoService" should "parse Log table" in {
    try {
      JDBCUtils.executeSql(url)(
        s"""CREATE TABLE default.log_table ON CLUSTER $clusterName (
           |a Int32
           |) ENGINE = Log
           |""".stripMargin)(_ => ())

      val table = TableInfoService.readTableInfo(url, "default.log_table", new Properties()).get

      table.isInstanceOf[GenericTable] should be (true)
      table.database should be ("default")
      table.name should be ("log_table")
      table.engine should be ("Log")
    } finally {
      JDBCUtils.executeSql(url)(s"DROP TABLE default.log_table ON CLUSTER $clusterName SYNC")(_ => ())
    }
  }

  "TableInfoService" should "parse MergeTree table without sorting key" in {
    try{
      JDBCUtils.executeSql(url)(
        s"""CREATE TABLE default.mt_table ON CLUSTER $clusterName (
           |a Int32
           |) ENGINE = MergeTree() order by tuple()
           |""".stripMargin)(_ => ())

      val table = TableInfoService.readTableInfo(url, "default.mt_table", new Properties()).get.asInstanceOf[MergeTreeTable]

      table.isInstanceOf[MergeTreeTable] should be (true)
      table.database should be("default")
      table.engine should be ("MergeTree")
      table.name should be("mt_table")
      table.orderingKey.isEmpty should be (true)
    } finally {
      JDBCUtils.executeSql(url)(s"DROP TABLE default.mt_table ON CLUSTER $clusterName SYNC")(_ => ())
    }
  }

  "TableInfoService" should "parse MergeTree table with sorting key" in {
    try {
      JDBCUtils.executeSql(url)(
        s"""CREATE TABLE default.mt_table ON CLUSTER $clusterName (
           |a Int32,
           |b String
           |) ENGINE = ReplacingMergeTree() ORDER BY (a, b)
           |""".stripMargin)(_ => ())

      val table = TableInfoService.readTableInfo(url, "default.mt_table", new Properties()).get.asInstanceOf[MergeTreeTable]

      table.isInstanceOf[MergeTreeTable] should be(true)
      table.database should be("default")
      table.name should be("mt_table")
      table.engine should be ("ReplacingMergeTree")
      table.orderingKey should be (Some("a, b"))
    } finally {
      JDBCUtils.executeSql(url)(s"DROP TABLE default.mt_table ON CLUSTER $clusterName SYNC")(_ => ())
    }
  }

  "TableInfoService" should "parse Distributed table and underlying MergeTree table" in {
    try {
      JDBCUtils.executeSql(url)(
        s"""CREATE TABLE default.mt_table ON CLUSTER $clusterName (
           |a Int32,
           |b String
           |) ENGINE = ReplacingMergeTree() ORDER BY (abs(a), b)
           |""".stripMargin)(_ => ())

      JDBCUtils.executeSql(url)(
        s"""CREATE TABLE default.dstr_table ON CLUSTER $clusterName (
           |a Int32,
           |b String
           |) ENGINE = Distributed($clusterName, default, mt_table)
           |""".stripMargin)(_ => ())

      val table = TableInfoService.readTableInfo(url, "default.dstr_table", new Properties()).get.asInstanceOf[DistributedTable]
      table.database should be ("default")
      table.name should be ("dstr_table")
      table.engine should be ("Distributed")
      val underlyingTable = table.underlyingTable.asInstanceOf[MergeTreeTable]
      underlyingTable.engine should be ("ReplacingMergeTree")
      underlyingTable.database should be ("default")
      underlyingTable.name should be ("mt_table")
      underlyingTable.orderingKey should be (Some("abs(a), b"))
    } finally {
      JDBCUtils.executeSql(url)(s"DROP TABLE default.dstr_table ON CLUSTER $clusterName SYNC")(_ => ())
      JDBCUtils.executeSql(url)(s"DROP TABLE default.mt_table ON CLUSTER $clusterName SYNC")(_ => ())
    }
  }
}
