package ann4s

import org.scalatest._
import org.scalatest.Matchers._

import scala.util.{Random => R}

class CompactVectorTest extends FunSuite {

  def nrm2(ar: Array[Float]): Float = math.sqrt(ar.map(x => x * x).sum).toFloat

  def cosine(ar1: Array[Float], ar2: Array[Float]): Float = {
    val n1 = nrm2(ar1)
    val n2 = nrm2(ar2)
    ar1.zip(ar2).map(v => v._1 * v._2 / n1 / n2).sum
  }

  test("MSE of CompactVector is less than tolerance") {
    val rnd = new R()
    val n = 100
    var d = 2
    while (d <= 2048) {
      val tolerance = math.sqrt(1e-5 * d)
      var mse = 0.0
      0 until n foreach { _ =>
        val ar = Array.fill(d)(rnd.nextFloat())
        val cv = CompactVector(ar)
        mse += cv.euclideanDistance(ar)
      }
      mse /= n
      mse should be (0.0 +- tolerance)
      d <<= 2
    }
  }

  test("Difference of Cosine Distance between CompactVector is less than torerance") {
    val rnd = new R()
    val n = 100
    var d = 2
    while (d <= 2048) {
      val tolerance = math.sqrt(1e-5 * d)
      var diff = 0.0
      0 until n foreach { _ =>
        val ar1 = Array.fill(d)(rnd.nextFloat())
        val ar2 = Array.fill(d)(rnd.nextFloat())
        val cv1 = CompactVector(ar1)
        val tr = 2 - 2 * cosine(ar1, ar2)
        val ts = cv1.cosineDistance(ar2, nrm2(ar2))
        diff += math.abs(tr - ts)
      }
      diff /= n
      diff should be (0.0 +- tolerance)
      d <<= 2
    }
  }

}
