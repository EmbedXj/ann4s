package ann4s

import scala.collection.mutable
import scala.util.Random

object CosineTree {

  val iterationSteps = 200

  def twoMeans(samples: Array[CompactVector]): (Array[Float], Array[Float]) = {
    val p = samples(0).unitVector()
    val q = samples(1).unitVector()
    var ic = 1
    var jc = 1

    samples.slice(2, iterationSteps + 2).foreach { r =>
      val di = ic * r.cosineDistance(p)
      val dj = jc * r.cosineDistance(q)
      if (di < dj) {
        var z = 0
        while (z < p.length) {
          p(z) = (p(z) * ic + r.unit(z)) / (ic + 1)
          z += 1
        }
        ic += 1
      } else if (dj < di) {
        var z = 0
        while (z < q.length) {
          q(z) = (q(z) * jc + r.unit(z)) / (jc + 1)
          z += 1
        }
        jc += 1
      }
    }
    (p, q)
  }

  def createSplit(sample: Array[CompactVector]): CompactVector = {
    val (p, q) = twoMeans(sample)
    var z = 0
    while (z < p.length) {
      p(z) -= q(z)
      z += 1
    }
    CompactVector(p, toUnit = true)
  }

  def margin(m: CompactVector, n: CompactVector): Float = {
    var dot = 0f
    val d = m.d
    var z = 0
    while (z < d) {
      dot += m(z) * n(z)
      z += 1
    }
    dot
  }

  def side(m: CompactVector, n: CompactVector): Boolean = {
    val dot = margin(m, n)
    if (dot != 0)
      dot > 0
    else
      Random.nextBoolean()
  }

  def traverse(tree: Map[BigInt, CompactVector], cv: CompactVector): BigInt = {
    var leaf = BigInt(0)
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

  def traverse(tree: mutable.Map[BigInt, CompactVector], cv: CompactVector): BigInt = {
    var leaf = BigInt(0)
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

class CosineTree(numItems: Long, steps: Int, l: Int, sampleRate: Double) extends Serializable {

  var done = numItems <= l

  var depth: Int = 0

  def finished(): Boolean = done || depth > steps

  val tree: mutable.Map[BigInt, CompactVector] = mutable.Map.empty

  // leaf id increases as 2^{0} + 2^{1} + ... + 2^{numSplits}
  var countByLeaf: Map[BigInt, Long] = Map(BigInt(0) -> numItems)

  def split[L <: { def log(msg: String): Unit }](groupedSamples: Iterator[(BigInt, Array[CompactVector])], logger: L): Unit = {
    val newCountByLeaf = mutable.Map[BigInt, Long]()

    groupedSamples foreach { case (leaf, samples) =>
      val approxCount = math.round(samples.length / sampleRate)
      logger.log(s"$leaf:$approxCount")
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
      // never updated
      done = true
    }
  }

  def traverse(cv: CompactVector): BigInt = CosineTree.traverse(tree, cv)

  def getTree: Map[BigInt, CompactVector] = tree.toMap

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


