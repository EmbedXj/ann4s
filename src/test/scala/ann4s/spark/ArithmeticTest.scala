package ann4s.spark

import com.github.fommil.netlib.BLAS
import org.apache.spark.ml.nn.IdVectorWithNorm
import org.scalatest.Matchers._
import org.scalatest._

import scala.util.Random

class ArithmeticTest extends FunSuite {

  val random = new Random(0x816)
  val blas = BLAS.getInstance()

  test("IndexedVectorWithNorm: norm") {
    val ar = Array.fill(64)(Random.nextDouble())
    val v = IdVectorWithNorm(0, ar)
    val nrm2 = blas.dnrm2(ar.length, ar, 1)
    v.norm should be (nrm2 +- 1e-8)
  }

  test("IndexedVectorWithNorm: copy") {
    val ar = Array.fill(64)(Random.nextDouble())
    val v = IdVectorWithNorm(0, ar)
    val original = v.vector
    val copied = v.copyMutableVectorWithNorm.vector
    assert(original ne copied)
  }

  test("VectorWithNorm: copy") {
    val ar1 = Array.fill(64)(2.0)
    val ar2 = Array.fill(64)(3.0)
    val v1 = IdVectorWithNorm(1, ar1)
    val v2 = IdVectorWithNorm(2, ar2)
    val m = v1.copyMutableVectorWithNorm

    m.aggregate(v2, 1)

    val expected = Array.fill(64)(0.5625)
    val result = m.vector.values

    result should be (expected)
  }

}
