package ann4s

import scala.collection.mutable
import scala.util.Random

object CosineTree {

  def twoMeans(samples: Array[CompactVector]): (Array[Float], Array[Float]) = {
    val p = samples(0).unitVector()
    val q = samples(1).unitVector()
    var ic = 1
    var jc = 1

    samples.drop(2).foreach { r =>
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
}

class CosineTree(f: Int, count: Long, numItemsInLeaf: Int, sampleSize: Int, sampleTolerance: Double) extends Serializable {

  def info() = {
    val maxChildren = countByLeaf.toSeq.maxBy(_._2)
    println(s"num leaves ${countByLeaf.size}, $maxChildren")
  }

  assert (sampleTolerance > 0 && sampleTolerance < 1)

  var done = false

  var numSplits: Int = 0

  def finished(): Boolean = done || numSplits > 29 // 29 for Int.MaxValue

  val tree: mutable.Map[Int, CompactVector] = mutable.Map.empty[Int, CompactVector]

  // leaf id increases as 2^{0} + 2^{1} + ... + 2^{numSplits}
  var countByLeaf: Map[Int, Long] = Map(0 -> count)

  def sample(cv: CompactVector): Option[(Int, CompactVector)] = {
    val leaf = traverse(cv)
    val n = countByLeaf(leaf)
    if (n <= numItemsInLeaf) {
      None // do not split
    } else {
      val fraction = sampleSize * (1 + sampleTolerance) / n.toDouble
      if (Random.nextDouble() < fraction) {
        Some(leaf -> cv)
      } else {
        None // not a sample
      }
    }
  }

  def needSplitMore(subtreeId: Int): Boolean = {
    countByLeaf(subtreeId) > numItemsInLeaf
  }

  def createSplit(sampleByLeaf: Map[Int, Array[CompactVector]]): Boolean = {
    var exactCountRequired = false
    val newCountByLeaf = mutable.Map[Int, Long]()
    if (sampleByLeaf.isEmpty) {
      done = true
    } else {
      sampleByLeaf.toSeq.sortBy(_._1) foreach { case (leaf, samples) =>

        newCountByLeaf += ((leaf << 1) + 1) -> (countByLeaf(leaf) / 2)
        newCountByLeaf += ((leaf << 1) + 2) -> (countByLeaf(leaf) / 2)

        if (samples.length < (sampleSize * (1 - sampleTolerance))) {
          assert(false)
          exactCountRequired = true
        } else {
          val hyperplane = CosineTree.createSplit(samples)
          tree += leaf -> hyperplane
        }
      }
    }

    numSplits += 1

    countByLeaf = newCountByLeaf.toMap
    exactCountRequired
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

  def traverse(cv: CompactVector): Int = {
    var leaf = 0
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

  def updateExactCountByLeaf(count: Map[Int, Long]): Unit = {
    countByLeaf = count
  }

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


