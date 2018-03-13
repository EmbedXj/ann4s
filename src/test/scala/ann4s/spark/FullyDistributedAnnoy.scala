package ann4s.spark

import java.io.FileInputStream
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.nn.{ANN, ANNModel, NN}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object FullyDistributedAnnoy {

  def loadNeighbors(): Array[Array[Int]] = {
    val fis = try {
      new FileInputStream(s"dataset/neighbors.bin")
    } catch { case ex: Throwable =>
      println("run `dataset/download.sh` in shell first")
      throw ex
    }

    val data = new Array[Byte](100 * 4)

    val dataset = new ArrayBuffer[Array[Int]]()
    var n = 0
    while (fis.read(data) == 100 * 4) {
      val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      dataset += Array.fill(100)(bf.getInt)
    }
    fis.close()
    dataset.toArray
  }

  def loadDistances(): Array[Array[Float]] = {
    val fis = try {
      new FileInputStream(s"dataset/distances.bin")
    } catch { case ex: Throwable =>
      println("run `dataset/download.sh` in shell first")
      throw ex
    }

    val data = new Array[Byte](100 * 4)

    val dataset = new ArrayBuffer[Array[Float]]()
    var n = 0
    while (fis.read(data) == 100 * 4) {
      val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      dataset += Array.fill(100)(bf.getFloat)
    }
    fis.close()
    dataset.toArray
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark.ml").setLevel(Level.DEBUG)

    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("building index")
      .getOrCreate()

    val ann = new ANN()
      .setFeaturesCol("features")
      .setL(100000)

    val trainData = spark.read.parquet("dataset/train")
    val testData = spark.read.parquet("dataset/test")

    val annModel = ann.fit(trainData)

    annModel.write.overwrite().save("exp/ann")

    val loaded = ANNModel.load("exp/ann")

    val namedTrainData = trainData
      .withColumn("target", lit("train"))

    val namedTestData = testData
      .withColumn("target", lit("test"))

    val nns = loaded
      .setTargetCol("target")
      .setTrainVal("train")
      .setTestVal("test")
      .setK(100)
      .transform(namedTrainData.union(namedTestData))

    val trueNeighbors = loadNeighbors()

    var n = 0
    var hit = 0
    import spark.implicits._
    nns.as[NN].map(x => x.id -> x.neighbors).collect() foreach { case (id, neighbors) =>
      val trueResult = trueNeighbors(id)
      n += math.min(trueResult.length, neighbors.length)
      hit += trueResult.toSet.intersect(neighbors.toSet).size
    }
    println(s"recall: ${hit / n.toDouble}")

    spark.stop()
  }

}

