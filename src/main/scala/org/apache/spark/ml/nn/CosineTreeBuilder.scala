package org.apache.spark.ml.nn

import org.apache.spark.ml.linalg.{BLAS, DenseVector, Vector, Vectors}

import scala.collection.mutable
import scala.util.Random

object CosineTree {

  val iterationSteps = 200

  def toUnitVector(v: Vector): Vector = {
    val nrm2 = Vectors.norm(v, 2)
    BLAS.scal(1 / nrm2, v)
    v
  }

  def cosineDistance(a: Vector, _anrm2: Double, b: Vector, _bnrm2: Double): Double = {
    val anrm2 = if (_anrm2 > 0) _anrm2 else Vectors.norm(a, 2)
    val bnrm2 = if (_bnrm2 > 0) _bnrm2 else Vectors.norm(b, 2)
    var dot = BLAS.dot(a, b)
    if (anrm2 > 1e-8)
      dot /= anrm2
    if (bnrm2 > 1e-8)
      dot /= bnrm2
    2 - 2 * dot
  }

  def twoMeans(samples: Array[Vector]): (DenseVector, DenseVector) = {

    Vectors.norm(samples(0), 2)

    val p = toUnitVector(samples(0)).toDense
    val q = toUnitVector(samples(1)).toDense
    val pv = p.values // get underlying for update
    val qv = q.values // get underlying for update
    var ic = 1
    var jc = 1

    samples.slice(2, iterationSteps + 2).foreach { a =>
      val r = toUnitVector(a)
      val di = ic * cosineDistance(r, 1, p, -1)
      val dj = jc * cosineDistance(r, 1, q, -1)
      if (di < dj) {
        r.foreachActive { (z, v) =>
          pv(z) = (pv(z) * ic + v) / (ic + 1)
        }
        ic += 1
      } else if (dj < di) {
        r.foreachActive { (z, v) =>
          qv(z) = (qv(z) * jc + v) / (jc + 1)
        }
        jc += 1
      }
    }
    (p, q)
  }

  def createSplit(sample: Array[Vector]): Vector = {
    val (p, q) = twoMeans(sample)
    val pv = p.values // get underlying for update
    var z = 0
    while (z < p.size) {
      pv(z) -= q(z)
      z += 1
    }
    toUnitVector(p)
  }

  def margin(m: Vector, n: Vector): Double = BLAS.dot(m, n)

  def side(m: Vector, n: Vector): Boolean = {
    val dot = margin(m, n)
    if (dot != 0)
      dot > 0
    else
      Random.nextBoolean()
  }

  def traverse(tree: Map[Long, Vector], cv: Vector): Long = {
    var leaf = 0L
    while (tree.contains(leaf)) {
      if (side(tree(leaf), cv)) {
        // true -> left
        leaf = (leaf << 1) + 1
      } else {
        // false -> right
        leaf = (leaf << 1) + 2
      }
    }
    leaf
  }

  def traverse(tree: mutable.Map[Long, Vector], cv: Vector): Long = {
    var leaf = 0L
    while (tree.contains(leaf)) {
      if (side(tree(leaf), cv)) {
        // true -> left
        leaf = (leaf << 1) + 1
      } else {
        // false -> right
        leaf = (leaf << 1) + 2
      }
    }
    leaf
  }

}

case class CosineTree(tree: Map[Long, Vector]) {
  def traverse(v: Vector): Long = CosineTree.traverse(tree, v)
}

class CosineTreeBuilder(numItems: Long, steps: Int, l: Int, sampleRate: Double) extends Serializable {

  var done = numItems <= l

  var depth: Int = 0

  def finished(): Boolean = done || depth > steps

  val tree: mutable.Map[Long, Vector] = mutable.Map.empty

  // leaf id increases as 2^{0} + 2^{1} + ... + 2^{numSplits}
  var countByLeaf: Map[Long, Long] = Map(0L -> numItems)

  def split[L <: { def log(msg: String): Unit }](groupedSamples: Iterator[(Long, Array[Vector])], logger: L): Unit = {
    val newCountByLeaf = mutable.Map[Long, Long]()

    groupedSamples foreach { case (leaf, samples) =>
      val approxCount = math.round(samples.length / sampleRate)
      logger.log(s"$depth:$leaf:${samples.length}:$approxCount")
      if (approxCount > l) {
        tree += leaf -> CosineTree.createSplit(samples)

        // update count
        newCountByLeaf += ((leaf << 1) + 1) -> (approxCount / 2)
        newCountByLeaf += ((leaf << 1) + 2) -> (approxCount / 2)
      }
    }

    if (newCountByLeaf.nonEmpty) {
      depth += 1
      countByLeaf = newCountByLeaf.toMap
    } else {
      logger.log("nothing to split")
      done = true
    }
  }

  def traverse(cv: Vector): Long = CosineTree.traverse(tree, cv)

  def result(): CosineTree = CosineTree(tree.toMap)

  def printTree(): Unit = {
    if (tree.contains(0))
      print(0, 0)
  }

  def print(root: Int, depth: Int): Unit = {
    println("        " * depth + f"${(root - 1)/2}%5d:$root%-5d")
    val l = (root << 1) + 1
    val r = (root << 1) + 2
    if (tree.contains(l)) {
      print(l, depth + 1)
    }
    if (tree.contains(r)) {
      print(r, depth + 1)
    }
  }

}


