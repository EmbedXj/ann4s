package ann4s

import java.io.FileOutputStream

import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class AnnoyIndex(f: Int, distance: Distance, random: Random) {

  def this(f: Int, distance: Distance, seed: Long) = this(f, distance, new Random(seed))

  def this(f: Int, distance: Distance) = this(f, distance, new Random)

  private val items = new ArrayBuffer[IdVector]()

  private var underlying: Index = _

  def addItem(i: Int, v: Array[Double]): Unit = {
    items += IdVector(i, Vectors.dense(v))
  }

  def build(numTrees: Int): Unit = {
    assert(underlying == null)
    val d = items.head.vector.size
    val idVectorWithNorm = items.map(_.toIdVectorWithNorm)
    underlying = new IndexBuilder(numTrees, d + 2)(distance, random).build(idVectorWithNorm)
  }

  def save(filename: String): Unit = {
    assert(underlying != null)
    val d = items.head.vector.size
    val fos = new FileOutputStream(filename)
    underlying.writeAnnoyBinary(d, fos)
    fos.close()
  }

}
