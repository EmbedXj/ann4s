package ann4s.spark

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.nn.{IdVectorWithNorm, Index, IndexAggregator, IndexBuilder}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object LocalBuilds {

  def main(args: Array[String]): Unit = {

    val d = 25
    val data = new Array[Byte](d * 4)
    val ar = new Array[Float](d)

    val fis = new FileInputStream(s"dataset/train.bin")
    val items = new ArrayBuffer[IdVectorWithNorm]()
    var id = 0
    while (fis.read(data) == d * 4) {
      val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      while (i < d) {
        ar(i) = bf.getFloat()
        i += 1
      }

      val copied = ar.map(_.toDouble)
      val cv = Vectors.dense(copied)
      items += IdVectorWithNorm(id, cv)
      id += 1
      if ((id % 10000) == 0)
        println(id)
    }
    fis.close()

    implicit val random = new Random(new Kiss32Random)

    val builder = new IndexBuilder(1, d + 2)

    val samples = Array.fill(10000)(items(Random.nextInt(items.length)))

    val masterIndex = new Index(builder.build(samples).toArray, false)

    val aggregator = new IndexAggregator
    aggregator.aggregate(masterIndex.nodes)

    items
      .map { item =>
        masterIndex.traverse(item.vector) -> item
      }
      .groupBy(_._1)
      .map { case (subTreeId, it) =>
        println(subTreeId, it.length)
        subTreeId -> new IndexBuilder(1, d + 2).build(it.map(_._2))
      }
      .foreach { case (subTreeId, subTreeNodes) =>
        aggregator.mergeSubTree(subTreeId, subTreeNodes)
      }



    val index = aggregator.prependItems2(items).result()

    val directory = new File("exp/annoy")
    directory.mkdirs()
    val os = new FileOutputStream(new File(directory, "local.ann"))
    index.writeAnnoyBinary(d, os)
    os.close()
  }

}
