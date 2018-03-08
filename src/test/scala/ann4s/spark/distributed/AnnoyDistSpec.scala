//package ann4s.spark.distributed
//
//import ann4s.Random
//import ann4s.spark.LocalSparkContext
//import org.apache.spark.sql.SQLContext
//import org.scalatest.{FlatSpec, Matchers}
//
//class AnnoyDistSpec extends FlatSpec with Matchers with LocalSparkContext {
//
//  import ann4s.profiling.AnnoyDataset.{dataset => features}
//
//  object FixRandom extends Random {
//    val rnd = new scala.util.Random(0)
//    override def flip(): Boolean = rnd.nextBoolean()
//    override def index(n: Int): Int = rnd.nextInt(n)
//  }
//
//  "Spark DataFrame-API" should "work" in {
//    val sqlContext = new SQLContext(sc)
//
//    val idCol = "id"
//    val featuresCol = "features"
//    val neighborCol = "neighbor"
//    val dimension = features.head.length
//
//
//
//    /*
//    val rdd: RDD[(Int, Array[Float])] =
//      sc.parallelize(features.zipWithIndex.map(_.swap))
//
//    val dataset: DataFrame = rdd.toDF(idCol, featuresCol)
//
//    val annoyModel: AnnoyModel = new Annoy()
//      .setDimension(dimension)
//      .setIdCol(idCol)
//      .setFeaturesCol(featuresCol)
//      .setNeighborCol(neighborCol)
//      .setDebug(true)
//      .fit(dataset)
//
//    annoyModel
//      .write
//      .overwrite
//      .save("annoy-spark-result")
//
//    val loadedModel = AnnoyModel
//      .read
//      .context(sqlContext)
//      .load("annoy-spark-result")
//
//    val result: DataFrame = loadedModel
//      .setK(10) // find 10 neighbors
//      .transform(dataset)
//
//    result.show()
//
//    result.select(idCol, neighborCol)
//      .map { case Row(id: Int, neighbor: Int) =>
//        (id, neighbor)
//      }
//      .groupByKey()
//      .collect()
//      .foreach { case (id, nns) =>
//        nns.toSeq.intersect(trueNns(id)).length should be >= 2
//      }
//      */
//  }
//
//}
//
