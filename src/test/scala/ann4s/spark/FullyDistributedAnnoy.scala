package ann4s.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.nn.{ANN, ANNModel}
import org.apache.spark.sql.SparkSession

object FullyDistributedAnnoy {

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
      .setPredictionCol("leaf")
      .setSteps(10)
      .setL(100000)
      .setSampleRate(0.1)

    val data = spark.read.parquet("dataset/train")

    val annModel = ann.fit(data)

    annModel.write.overwrite().save("exp/ann")

    val loaded = ANNModel.load("exp/ann")

    spark.stop()
  }

}

