package ann4s

import org.apache.spark.ml.nn.BLASProxy
import org.apache.spark.ml.linalg.Vector

import scala.util.Random

object Side extends Enumeration {

  val Left, Right = Value

  def side(b: Boolean): Value = if (!b) Left else Right

}

trait Distance {

  def distance(a: VectorWithNorm, b: IdVectorWithNorm): Double

  def distance(a: IdVectorWithNorm, b: IdVectorWithNorm): Double

  def margin(m: Vector, n: Vector): Double

  def side(m: Vector, n: Vector)(implicit random: Random): Side.Value

}

object CosineDistance extends Distance {

  def distance(a: VectorWithNorm, b: IdVectorWithNorm): Double = {
    val dot = BLASProxy.dot(a.vector, b.vector)
    val norm = a.norm * b.norm
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def distance(a: IdVectorWithNorm, b: IdVectorWithNorm): Double = {
    val dot = BLASProxy.dot(a.vector, b.vector)
    val norm = a.norm * b.norm
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def margin(m: Vector, n: Vector): Double = BLASProxy.dot(m, n)

  def side(m: Vector, n: Vector)(implicit random: Random): Side.Value = {
    val dot = margin(m, n)
    if (dot == 0) Side.side(random.nextBoolean())
    else if (dot > 0) Side.Right
    else Side.Left
  }
}
