package ann4s

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.nn.BLASProxy

trait HasId {
  def getId: Int
}

trait HasVector {
  def getVector: Vector
}

trait HasNorm {
  def getNorm: Double
}

case class IdVector(id: Int, vector: Vector) extends HasId with HasVector {
  def toIdVectorWithNorm: IdVectorWithNorm = IdVectorWithNorm(id, vector)

  override def getId: Int = id
  override def getVector: Vector = vector
}

class VectorWithNorm(val vector: DenseVector, var norm: Double)
  extends HasVector with HasNorm with Serializable {

  def aggregate(other: IdVectorWithNorm, c: Int): this.type = {
    BLASProxy.scal(c, vector)
    BLASProxy.axpy(1.0 / other.norm, other.vector, vector)
    BLASProxy.scal(1.0 / (c + 1), vector)
    norm = Vectors.norm(vector, 2)
    this
  }

  override def getVector: Vector = vector
  override def getNorm: Double = norm
}

case class IdVectorWithNorm(id: Int, vector: Vector, norm: Double)
  extends HasId with HasVector with HasNorm {

  def copyVectorWithNorm: VectorWithNorm = {
    vector match {
      case sv: SparseVector =>
        val copied = sv.toDense
        val norm = Vectors.norm(copied, 2)
        BLASProxy.scal(1 / norm, copied)
        new VectorWithNorm(copied, 1)
      case dv: DenseVector =>
        val copied = dv.copy
        val norm = Vectors.norm(copied, 2)
        BLASProxy.scal(1 / norm, copied)
        new VectorWithNorm(copied, 1)
    }
  }

  override def getId: Int = id
  override def getVector: Vector = vector
  override def getNorm: Double = norm
}

object IdVectorWithNorm {

  def apply(id: Int, vector: Vector): IdVectorWithNorm =
    IdVectorWithNorm(id, vector, Vectors.norm(vector, 2.0))

  def apply(id: Int, array: Array[Double]): IdVectorWithNorm =
    IdVectorWithNorm(id, Vectors.dense(array))

}
