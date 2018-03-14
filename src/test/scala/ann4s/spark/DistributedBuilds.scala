package ann4s.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.nn.ANN
import org.apache.spark.sql.SparkSession

object DistributedBuilds {

  def main(args: Array[String]): Unit = {

    // turn off log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark.ui").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.ml").setLevel(Level.DEBUG)

    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("distributed builds")
      .getOrCreate()

    val ann = new ANN()
      .setFeaturesCol("features")
      .setNumTrees(1)

    val trainData = spark.read.parquet("dataset/train")

    val annModel = ann.fit(trainData)

    annModel.write.overwrite().save("exp/ann")

    spark.stop()
  }

}

