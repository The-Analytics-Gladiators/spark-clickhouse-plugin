package org.apache.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

import java.sql.{Connection, ResultSet}
import scala.reflect.ClassTag

object NotImplementedPartitioning extends Partition {
  override def index: Int = 0
}

class ClickhouseRdd[T: ClassTag](
                                  sc: SparkContext,
                                  getConnection: () => Connection,
                                  sql: String,
                                  mapRow: ResultSet => T)
  extends RDD[T](sc, Nil) {

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = new NextIterator[T]
  {

    context.addTaskCompletionListener[Unit]{ _ => closeIfNeeded() }
    val conn = getConnection()
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    stmt.setFetchSize(300)
    logInfo(s"statement fetch size set to: ${stmt.getFetchSize}")

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

  override protected def getPartitions: Array[Partition] = Array(NotImplementedPartitioning)
}
