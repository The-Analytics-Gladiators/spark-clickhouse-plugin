import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.flatspec.AnyFlatSpec

class DummyTest extends AnyFlatSpec with SharedSparkContext {
  "spark" should "run" in {
     val set = sc.parallelize(Seq(1, 2, 3))
       .collect().toSet

    assert(set == Set(1, 2, 3))
  }
}
