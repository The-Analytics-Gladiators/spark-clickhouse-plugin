package com.blackmorse.spark.clickhouse.tables

sealed trait ClickhouseTable {
  val database: String
  val name: String
  val engine: String

  override def toString = s"$database.$name"
}

case class MergeTreeTable(database: String,
                          name: String,
                          engine: String,
                          orderingKey: Option[String]) extends ClickhouseTable

case class DistributedTable(database: String,
                            name: String,
                            engine: String,
                            cluster: String,
                            underlyingTable: ClickhouseTable) extends ClickhouseTable

case class GenericTable(database: String,
                        name: String,
                        engine: String) extends ClickhouseTable