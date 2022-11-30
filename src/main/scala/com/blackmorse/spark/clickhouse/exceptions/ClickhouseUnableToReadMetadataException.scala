package com.blackmorse.spark.clickhouse.exceptions

case class ClickhouseUnableToReadMetadataException(msg: String, cause: Throwable) extends Exception(msg, cause)
