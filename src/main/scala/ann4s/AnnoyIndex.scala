package ann4s

import java.io.FileOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

@deprecated("take a look at LocalBuilds in test")
class AnnoyIndex(f: Int, distance: Distance, random: Random) {

  def this(f: Int, distance: Distance, seed: Long) = this(f, distance, new Random(seed))

  def this(f: Int, distance: Distance) = this(f, distance, new Random)

  private val items = new ArrayBuffer[IdVector]()

  private var underlying: Index = _

  def addItem(i: Int, v: Array[Float]): Unit = {
    items += IdVector(i, SVector(v))
  }

  def build(numTrees: Int): Unit = {
    assert(underlying == null)
    val d = items.head.vector.size
    val idVectorWithNorm = items.map(_.toIdVectorWithNorm)
    val index = new IndexBuilder(numTrees, d + 2)(distance, random).build(idVectorWithNorm)
    underlying = new IndexAggregator().prependItems(items).aggregate(index.nodes).result()
  }

  def save(filename: String): Unit = {
    assert(underlying != null)
    val d = items.head.vector.size
    val fos = new FileOutputStream(filename)
    underlying.writeAnnoyBinary(d, fos)
    fos.close()
  }

}
