package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

import java.sql.{Connection, ResultSet}
import scala.reflect.ClassTag

private case class ClickhousePartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index: Int = idx
}

class ClickhouseRdd[T: ClassTag](
                                  sc: SparkContext,
                                  getConnection: () => Connection,
                                  sql: String,
                                  lowerBound: Long,
                                  upperBound: Long,
                                  numPartitions: Int,
                                  mapRow: ResultSet => T)
  extends RDD[T](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = new NextIterator[T]
  {

    context.addTaskCompletionListener[Unit]{ _ => closeIfNeeded() }
    val part = split.asInstanceOf[ClickhousePartition]
    val conn = getConnection()
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    stmt.setFetchSize(300)
    logInfo(s"statement fetch size set to: ${stmt.getFetchSize}")

    stmt.setLong(1, part.lower)
    stmt.setLong(2, part.upper)
    val rs = stmt.executeQuery()

    override def getNext(): T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close(): Unit = {
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }

  override protected def getPartitions: Array[Partition] = {
    // bounds are inclusive, hence the + 1 here and - 1 on end
    val length = BigInt(1) + upperBound - lowerBound
    (0 until numPartitions).map { i =>
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      new ClickhousePartition(i, start.toLong, end.toLong)
    }.toArray
  }
}
