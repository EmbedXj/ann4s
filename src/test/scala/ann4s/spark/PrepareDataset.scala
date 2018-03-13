package ann4s.spark

import java.io.FileInputStream
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object PrepareDataset {

  case class DatasetWrapper(features: Vector)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("Prepare Dataset")
      .getOrCreate()

    import spark.implicits._

    val d = 25
    val data = new Array[Byte](d * 4)
    val ar = new Array[Float](d)

    Seq("train", "test") foreach { setName =>
      val fis = try {
        new FileInputStream(s"dataset/$setName.bin")
      } catch { case ex: Throwable =>
        println("run `dataset/download.sh` in shell first")
        throw ex
      }

      val dataset = new ArrayBuffer[DatasetWrapper]()
      var n = 0
      while (fis.read(data) == d * 4) {
        val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < d) {
          ar(i) = bf.getFloat()
          i += 1
        }

        val cv = Vectors.dense(ar.map(_.toDouble))
        dataset += DatasetWrapper(cv)
        n += 1
        if ((n % 10000) == 0)
          println(n)
      }
      fis.close()

      spark
        .sparkContext
        .parallelize(dataset)
        .toDS
        .write
        .mode("overwrite")
        .parquet(s"dataset/$setName")
    }

    spark.stop()
  }

}
