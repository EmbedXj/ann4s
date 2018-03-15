package org.apache.spark.ml.nn

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.ml.linalg.{BLAS, DenseVector, SparseVector, Vector, Vectors}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.interpreter.OutputStream
import scala.util.Random

class VectorWithNorm(val vector: DenseVector, var norm: Double) extends Serializable {

  def aggregate(other: IdVectorWithNorm, c: Int): Unit = {
    BLAS.scal(c, vector)
    BLAS.axpy(1.0 / other.norm, other.vector, vector)
    BLAS.scal(1.0 / (c + 1), vector)
    norm = Vectors.norm(vector, 2)
  }

}

case class IdVectorWithNorm(id: Int, vector: Vector, norm: Double) {

  def copyMutableVectorWithNorm: VectorWithNorm = {
    vector match {
      case sv: SparseVector =>
        new VectorWithNorm(sv.toDense, norm)
      case dv: DenseVector =>
        new VectorWithNorm(dv.copy, norm)
    }
  }

}

object IdVectorWithNorm {

  def apply(id: Int, vector: Vector): IdVectorWithNorm =
    IdVectorWithNorm(id, vector, Vectors.norm(vector, 2.0))

  def apply(id: Int, array: Array[Double]): IdVectorWithNorm =
    IdVectorWithNorm(id, Vectors.dense(array))

}

trait Node

case class RootNode(location: Int) extends Node

case class HyperplaneNode(hyperplane: DenseVector, l: Int, r: Int) extends Node

case class LeafNode(children: Array[Int]) extends Node

case class ItemNode(vector: Vector) extends Node

object CosineTree {

  val iterationSteps = 200

  def cosineDistance(a: VectorWithNorm, b: IdVectorWithNorm): Double = {
    val dot = BLAS.dot(a.vector, b.vector)
    val norm = a.norm * b.norm
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def cosineDistance(a: IdVectorWithNorm, b: IdVectorWithNorm): Double = {
    val dot = BLAS.dot(a.vector, b.vector)
    val norm = a.norm * b.norm
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def twoMeans(points: IndexedSeq[IdVectorWithNorm])(implicit random: Random): (DenseVector, DenseVector) = {
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

  def createSplit(sample: IndexedSeq[IdVectorWithNorm])(implicit random: Random): DenseVector = {
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

  def copyCosineForest(): Index = {
    val nodes: Array[Node] = this.nodes.map {
      case StructuredNode(nodeType, location, _, _, _) if nodeType == 1 =>
        RootNode(location)
      case StructuredNode(nodeType, l, r, hyperplane, _) if nodeType == 2 =>
        HyperplaneNode(new DenseVector(hyperplane), l, r)
      case StructuredNode(nodeType, _, _, _, children) if nodeType == 3 =>
        LeafNode(children)
    }
    new Index(nodes)
  }
}

class Index(val nodes: Array[Node], val withItems: Boolean) extends Serializable {

  def this(nodes: Array[Node]) = this(nodes, false)

  def writeAnnoyBinary(d: Int, os: OutputStream): Unit = {
    assert(withItems, "index should include items for Annoy")
    val bf = ByteBuffer.allocate(12 + d * 4).order(ByteOrder.LITTLE_ENDIAN)

    println(s"number of nodes ${nodes.length}")

    var numItemNodes = 0
    var numRootNodes = 0
    var numHyperplaneNodes = 0
    var numLeafNodes = 0
    nodes foreach {
      case ItemNode(vector) =>
        assert(numRootNodes == 0 && numHyperplaneNodes == 0 && numLeafNodes == 0)
        bf.clear()
        bf.putInt(1)
        bf.putInt(0) // TODO: fill out norm
        bf.putInt(0)
        for (x <- vector.toArray) bf.putFloat(x.toFloat)
        assert(bf.remaining() == 0)
        os.write(bf.array())
        numItemNodes += 1
      case RootNode(location) =>
        assert(numItemNodes > 0 && numHyperplaneNodes > 0 && numLeafNodes > 0)
        nodes(location) match {
          case HyperplaneNode(hyperplane, l, r) =>
            bf.clear()
            bf.putInt(numItemNodes)
            bf.putInt(l)
            bf.putInt(r)
            for (x <- hyperplane.toArray) bf.putFloat(x.toFloat)
            assert(bf.remaining() == 0)
            os.write(bf.array())
          case _ => assert(false)
        }
        numRootNodes += 1
      case HyperplaneNode(hyperplane, l, r) =>
        assert(numRootNodes == 0)
        bf.clear()
        bf.putInt(Int.MaxValue) // fake
        bf.putInt(l)
        bf.putInt(r)
        for (x <- hyperplane.toArray) bf.putFloat(x.toFloat)
        assert(bf.remaining() == 0)
        os.write(bf.array())
        numHyperplaneNodes += 1
      case LeafNode(children: Array[Int]) =>
        assert(numRootNodes == 0)
        bf.clear()
        bf.putInt(children.length)
        children foreach bf.putInt // if exceed, exception raised
        while (bf.remaining() > 0) bf.putInt(0) // fill 0s for safety
        assert(bf.remaining() == 0)
        os.write(bf.array())
        numLeafNodes += 1
    }
    println(numItemNodes, numRootNodes, numHyperplaneNodes, numLeafNodes)
  }

  def copyStructuredForest(): StructuredForest = {
    val structuredNodes = nodes.map {
      case RootNode(location) =>
        StructuredNode(1, location, -1, Array.emptyDoubleArray, Array.emptyIntArray)
      case HyperplaneNode(hyperplane, l, r) =>
        StructuredNode(2, l, r, hyperplane.values, Array.emptyIntArray)
      case LeafNode(children: Array[Int]) =>
        StructuredNode(3, -1, -1, Array.emptyDoubleArray, children)
    }
    StructuredForest(structuredNodes)
  }

  def getCandidates(v: IdVectorWithNorm): Array[Int] = ???

}



class IndexBuilder(numTrees: Int, leafNodeCapacity: Int, seed: Long) extends Serializable {

  def this(numTrees: Int, leafNodeCapacity: Int) = this(numTrees, leafNodeCapacity, Random.nextLong())

  assert(numTrees > 0)
  assert(leafNodeCapacity > 1)

  implicit val random: Random = new Random(seed)

  val nodes = new ArrayBuffer[Node]()

  def build(points: Array[IdVectorWithNorm]): IndexedSeq[Node] = {
    val roots = new ArrayBuffer[RootNode]()
    0 until numTrees foreach { _ =>
      val rootId = recurse(points)
      roots += RootNode(rootId)
    }
    nodes ++= roots
    nodes
  }

  def recurse(points: IndexedSeq[IdVectorWithNorm]): Int = {
    if (points.length <= leafNodeCapacity) {
      nodes += LeafNode(points.map(_.id).toArray)
      nodes.length - 1
    } else {
      val hyperplane = CosineTree.createSplit(points)
      val leftChildren = new ArrayBuffer[IdVectorWithNorm]
      val rightChildren = new ArrayBuffer[IdVectorWithNorm]
      points foreach { p =>
        if (CosineTree.side(hyperplane, p.vector)) leftChildren += p
        else rightChildren += p
      }

      if (leftChildren.isEmpty || rightChildren.isEmpty) {
        leftChildren.clear()
        rightChildren.clear()
        BLAS.scal(0, hyperplane) // set zeros
        points foreach { p =>
          if (random.nextBoolean()) leftChildren += p
          else rightChildren += p
        }
      }

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


