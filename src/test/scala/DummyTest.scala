import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Properties

class DummyTest extends AnyFlatSpec with DataFrameSuiteBase {
  "spark" should "run" in {
     val set = sc.parallelize(Seq(1, 2, 3, 4))
       .collect().toSet

    assert(set == Set(1, 2, 3, 4))
  }

  "Test clickhouse cluster" should "consists of 1 shard" in {
    import sqlContext.implicits._
    val count = sqlContext.read.jdbc("jdbc:clickhouse://localhost:8123", "system.clusters", new Properties())
      .count()
    // TODO 10 records for system.clusters table
    assert(count == 10L)
  }
}
