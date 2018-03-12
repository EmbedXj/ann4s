package ann4s

import scala.collection.mutable
import scala.util.Random

class CosineTree(count: Long, numItemsInLeaf: Int, sampleSize: Int, sampleTolerance: Double) extends Serializable {
  def printLeaves() = {
    println("print leaves")
    countByLeaf.toSeq.sortBy(_._1).foreach(println)
  }

  assert (sampleTolerance > 0 && sampleTolerance < 1)

  var done = false

  def finished(): Boolean = done

  val tree: mutable.Map[Int, CompactVector] = mutable.Map.empty[Int, CompactVector]

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

  def createSplit(sampleByLeaf: Map[Int, Array[CompactVector]]): Boolean = {
    var exactCountRequired = false
    val newCountByLeaf = mutable.Map[Int, Long]()
    if (sampleByLeaf.isEmpty) {
      println("No samples")
      done = true
    } else {
      sampleByLeaf.toSeq.sortBy(_._1) foreach { case (leaf, samples) =>

        newCountByLeaf += ((leaf << 1) + 1) -> (countByLeaf(leaf) / 2)
        newCountByLeaf += ((leaf << 1) + 2) -> (countByLeaf(leaf) / 2)

        if (samples.length < (sampleSize * (1 - sampleTolerance))) {
          println(s"leaf $leaf : ${samples.length} - not updated")
          exactCountRequired = true
        } else {
          println(s"leaf $leaf : ${samples.length}")
          // twoMeans
          val p = samples(0).unitVector()
          val q = samples(1).unitVector()
          var ic = 1
          var jc = 1
          samples.drop(2).foreach { r =>
            val di = ic * r.cosineDistance(p, 1)
            val dj = jc * r.cosineDistance(q, 1)

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
          // create split
          var z = 0
          while (z < p.length) {
            p(z) -= q(z)
            z += 1
          }
          tree += leaf -> CompactVector(p, toUnit = true)
        }
      }
    }

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

}
