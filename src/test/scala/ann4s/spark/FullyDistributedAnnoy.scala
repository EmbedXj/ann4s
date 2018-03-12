package ann4s.spark

import ann4s.{CompactVector, CosineTree}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FullyDistributedAnnoy {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("building index")
      .getOrCreate()

    import spark.implicits._

    val data = spark.read.parquet("dataset/train").as[CompactVector].cache()
    val count = data.count()

    val tree = new CosineTree(25, count, 10000, 200, 0.2)

    while (!tree.finished()) {
      tree.createSplit(data.flatMap(tree.sample).collect().groupBy(_._1).mapValues(_.map(_._2)))
      tree.updateExactCountByLeaf(data.groupByKey(tree.traverse).count().collect().toMap)
    }

    data.unpersist()
    spark.stop()
  }

}

