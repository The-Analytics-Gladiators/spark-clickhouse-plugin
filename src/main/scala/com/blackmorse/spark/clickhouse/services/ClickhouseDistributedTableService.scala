package com.blackmorse.spark.clickhouse.services

import com.blackmorse.spark.clickhouse.reader.TableInfo

import scala.annotation.tailrec

object ClickhouseDistributedTableService {
  private val distributedPrefix = "Distributed("
  private val orderBy = "ORDER BY "
  private val mergeTree = "MergeTree"

  def parseDistributedUnderlyingTable(engineFull: String): (String, String, String) = {
    val inside = engineFull.substring(distributedPrefix.length, engineFull.length - 1)
    val array = inside.split(",")
      .map(_.trim)
      .map(s => s.substring(1, s.length - 1))

    (array.head, array(1), array(2))
  }


  def parseOrderingKey(engineFull: String): Option[String] = {
    val startIndex = engineFull.indexOf(orderBy)
    if (startIndex == -1) None
    else if (engineFull(startIndex + orderBy.length) != '(') {
      Some(
        getWhatInside(engineFull.substring(startIndex + orderBy.length) + " ", 1, "", ' ', ' ')
      )
    } else Some(
      getWhatInside(engineFull.substring(startIndex + orderBy.length + 1), 1, "", '(', ')')
    )
  }

  def isMergeTree(tableInfo: TableInfo): Boolean = tableInfo.engine.endsWith(mergeTree)

  @tailrec
  private def getWhatInside(str: String, count: Int, result: String,
                            openSymbol: Char, closeSymbol: Char): String =
    str.headOption match {
      case None => throw new IllegalArgumentException(s"Can't parse ordering key from $str")
      case Some(`closeSymbol`) if count == 1 => result
      case Some(`closeSymbol`) => getWhatInside(str.tail, count - 1, result + closeSymbol, openSymbol, closeSymbol)
      case Some(`openSymbol`) => getWhatInside(str.tail, count + 1, result + openSymbol, openSymbol, closeSymbol)
      case Some(ch) => getWhatInside(str.tail, count, result + ch, openSymbol, closeSymbol)
    }
}
