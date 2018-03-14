package org.apache.spark.ml.nn

import org.apache.spark.ml.linalg.{BLAS, DenseVector, SparseVector, Vector, Vectors}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class VectorWithNorm(val vector: DenseVector, var norm: Double) extends Serializable {

  def aggregate(other: IndexedVectorWithNorm, c: Int): Unit = {
    BLAS.scal(c, vector)
    BLAS.axpy(1.0 / other.norm, other.vector, vector)
    BLAS.scal(1.0 / (c + 1), vector)
    norm = Vectors.norm(vector, 2)
  }

}

class IndexedVectorWithNorm(val id: Int, val vector: Vector, val norm: Double) extends Serializable {

  def this(id: Int, vector: Vector) = this(id, vector, Vectors.norm(vector, 2.0))

  def this(id: Int, array: Array[Double]) = this(id, Vectors.dense(array))

  def copyMutableVectorWithNorm: VectorWithNorm = {
    vector match {
      case sv: SparseVector =>
        new VectorWithNorm(sv.toDense, norm)
      case dv: DenseVector =>
        new VectorWithNorm(dv.copy, norm)
    }
  }

}

trait Node

case class RootNode(location: Int) extends Node

case class HyperplaneNode(hyperplane: DenseVector, l: Int, r: Int) extends Node

case class LeafNode(children: Array[Int]) extends Node

object CosineTree {

  val iterationSteps = 200

  def cosineDistance(a: VectorWithNorm, b: IndexedVectorWithNorm): Double = {
    val dot = BLAS.dot(a.vector, b.vector)
    val norm = a.norm * b.norm
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def twoMeans(points: IndexedSeq[IndexedVectorWithNorm])(implicit random: Random): (DenseVector, DenseVector) = {
    val count = points.length
    val i = random.nextInt(count)
    var j = random.nextInt(count - 1)
    j += (if (j >= i) 1 else 0)

    val p = points(i).copyMutableVectorWithNorm
    val q = points(j).copyMutableVectorWithNorm
    var ic = 1
    var jc = 1

    Iterator.fill(iterationSteps)(random.nextInt(points.length))
      .foreach { k =>
        val kp = points(k)
        if (kp.norm > 0) {
          val di = ic * cosineDistance(p, kp)
          val dj = jc * cosineDistance(q, kp)

          if (di < dj) {
            p.aggregate(kp, ic)
            ic += 1
          } else {
            q.aggregate(kp, jc)
            jc += 1
          }
        }
      }

    (p.vector, q.vector)
  }

  def createSplit(sample: IndexedSeq[IndexedVectorWithNorm])(implicit random: Random): DenseVector = {
    val (p, q) = twoMeans(sample)
    BLAS.axpy(-1, q, p)
    val norm = Vectors.norm(p, 2)
    BLAS.scal(1 / norm, p)
    p
  }

  def margin(m: Vector, n: Vector): Double = BLAS.dot(m, n)

  def side(m: Vector, n: Vector)(implicit random: Random): Boolean = {
    val dot = margin(m, n)
    if (dot != 0)
      dot > 0
    else
      random.nextBoolean()
  }

}

case class StructuredNode(nodeType: Int, l: Int, r: Int, hyperplane: Array[Double], children: Array[Int])

case class StructuredForest(nodes: Array[StructuredNode]) {

  def copyCosineForest(): CosineForest = {
    val nodes: Array[Node] = this.nodes.map {
      case StructuredNode(nodeType, l, r, hyperplane, _) if nodeType == 1 =>
        HyperplaneNode(new DenseVector(hyperplane), l, r)
      case StructuredNode(nodeType, _, _, _, children) if nodeType == 2 =>
        LeafNode(children)
      case StructuredNode(nodeType, location, _, _, _) if nodeType == 3 =>
        RootNode(location)
    }
    CosineForest(nodes)
  }
}

case class CosineForest(nodes: Array[Node]) {

  def copyStructuredForest(): StructuredForest = {
    val structuredNodes = nodes.map {
      case HyperplaneNode(hyperplane, l, r) =>
        StructuredNode(1, l, r, hyperplane.values, Array.emptyIntArray)
      case LeafNode(children: Array[Int]) =>
        StructuredNode(2, -1, -1, Array.emptyDoubleArray, children)
      case RootNode(location) =>
        StructuredNode(3, location, -1, Array.emptyDoubleArray, Array.emptyIntArray)
    }
    StructuredForest(structuredNodes)
  }

}

class CosineForestBuilder(numTrees: Int, leafNodeCapacity: Int, seed: Long) extends Serializable {

  def this(numTrees: Int, leafNodeCapacity: Int) = this(numTrees, leafNodeCapacity, Random.nextLong())

  assert(numTrees > 0)
  assert(leafNodeCapacity > 1)

  implicit val random: Random = new Random(seed)

  val nodes = new ArrayBuffer[Node]()

  def build(points: Array[IndexedVectorWithNorm]): CosineForest = {
    val roots = new ArrayBuffer[RootNode]()
    0 until numTrees foreach { _ =>
      val rootId = recurse(points)
      roots += RootNode(rootId)
    }
    nodes ++= roots
    CosineForest(nodes.toArray)
  }

  def recurse(points: IndexedSeq[IndexedVectorWithNorm]): Int = {
    if (points.length <= leafNodeCapacity) {
      nodes += LeafNode(points.map(_.id).toArray)
      nodes.length - 1
    } else {
      val hyperplane = CosineTree.createSplit(points)
      val leftChildren = new ArrayBuffer[IndexedVectorWithNorm]
      val rightChildren = new ArrayBuffer[IndexedVectorWithNorm]
      points foreach { p =>
        if (CosineTree.side(hyperplane, p.vector)) leftChildren += p
        else rightChildren += p
      }

      assert(leftChildren.nonEmpty && rightChildren.nonEmpty, s"TODO: handles this case, " +
        s"${nodes.length}, ${leftChildren.length}, ${rightChildren.length}")

      var l = -1
      var r = -1
      if (leftChildren.length < rightChildren.length) {
        l = recurse(leftChildren)
        r = recurse(rightChildren)
      } else {
        r = recurse(rightChildren)
        l = recurse(leftChildren)
      }

      nodes += HyperplaneNode(hyperplane, l, r)
      nodes.length - 1
    }
  }

}


