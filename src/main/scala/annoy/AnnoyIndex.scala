package annoy

import ann4s.RocksDBHelper

private[this] class AnnoyIndex(dim: Int, metric: String) {

  var pointer = if (metric == "angular") {
    AnnoyNative.annoyNative.createAngular(dim)
  } else if (metric == "euclidean") {
    AnnoyNative.annoyNative.createEuclidean(dim)
  } else {
    throw new IllegalArgumentException
  }

  def this(f: Int) = this(f, "angular")

  def addItem(item: Int, w: Array[Float]): Unit = {
    require(pointer != null)
    AnnoyNative.annoyNative.addItem(pointer, item, w)
  }

  def build(q: Int): Unit = {
    require(pointer != null)
    AnnoyNative.annoyNative.build(pointer, q)
  }

  def save(filename: String): Boolean = {
    require(pointer != null)
    AnnoyNative.annoyNative.save(pointer, filename)
  }

  def unload(): Unit = {
    require(pointer != null)
    AnnoyNative.annoyNative.unload(pointer)
  }

  def load(filename: String): Boolean = {
    require(pointer != null)
    AnnoyNative.annoyNative.load(pointer, filename)
  }

  def verbose(v: Boolean): Unit = {
    require(pointer != null)
    AnnoyNative.annoyNative.verbose(pointer, v)
  }

  def getNItems: Int = {
    require(pointer != null)
    AnnoyNative.annoyNative.getNItems(pointer)
  }

  def getItem(item: Int): Array[Float] = {
    require(pointer != null)
    val v = new Array[Float](dim)
    AnnoyNative.annoyNative.getItem(pointer, item, v)
    v
  }

  def getNnsByItem(item: Int, n: Int): Array[(Int, Float)] =
    getNnsByItem(item, n, -1)

  def getNnsByItem(item: Int, n: Int, k: Int): Array[(Int, Float)] = {
    require(pointer != null)
    val result = new Array[Int](n)
    val distances = new Array[Float](n)
    AnnoyNative.annoyNative.getNnsByItem(pointer, item, n, k, result, distances)
    result.zip(distances)
  }

  def getNnsByVector(w: Array[Float], n: Int): Array[(Int, Float)] = getNnsByVector(w, n, -1)

  def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    require(pointer != null)
    val result = new Array[Int](n)
    val distances = new Array[Float](n)
    AnnoyNative.annoyNative.getNnsByVector(pointer, w, n, k, result, distances)
    result.zip(distances)
  }

  def getDistance(i: Int, j: Int): Float = {
    require(pointer != null)
    AnnoyNative.annoyNative.getDistance(pointer, i, j)
  }

  def release(): Unit = {
    require(pointer != null)
    AnnoyNative.annoyNative.deleteIndex(pointer)
    pointer = null
  }

}

object AnnoyIndex {

  def withAnnoy[T](dim: Int, metric: String)(block: AnnoyIndex => T): T = {
    val annoyIndex = new AnnoyIndex(dim, metric)
    val result = block(annoyIndex)
    annoyIndex.unload()
    annoyIndex.release()
    result
  }

  def withAnnoy[T](dim: Int)(block: AnnoyIndex => T): T = {
    val annoyIndex = new AnnoyIndex(dim)
    val result = block(annoyIndex)
    annoyIndex.unload()
    annoyIndex.release()
    result
  }

}

object AnnoyIndexTest {

  def main(args: Array[String]): Unit = {
//    import scala.sys.process._
//    "make".!
    val f = 10
    val r = new scala.util.Random(0)
    AnnoyIndex.withAnnoy(f, "angular") { a =>
      RocksDBHelper.using(new ann4s.AnnoyIndex(f, ann4s.Angular, ann4s.FixRandom, "db")) { b =>
        (0 until 1000) foreach { i =>
          val v = Array.fill(f)(r.nextGaussian().toFloat)
          a.addItem(i, v)
          b.addItem(s"$i", v, s"$i")
        }
        a.build(1)
        b.build(1)
      }
    }

  }
}
