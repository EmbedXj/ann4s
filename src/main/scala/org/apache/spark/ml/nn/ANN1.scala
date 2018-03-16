package org.apache.spark.ml.nn

import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConversions._

import scala.util.Random

case class IdVector(id: Int, vector: Vector)

trait ANN1Params extends Params with HasFeaturesCol with HasSeed {

  final val idCol: Param[String] = new Param[String](this, "idCol", "id column name")

  def getIdCol: String = $(idCol)

  final val steps = new IntParam(this, "steps", "The number of steps" +
    "Must be > 0 and < 30.", ParamValidators.inRange(1, 29))

  def getSteps: Int = $(steps)

  final val l = new IntParam(this, "l", "max number of items in a leaf")

  def getL: Int = $(l)

  final val sampleRate = new DoubleParam(this, "sampling rate", "sampling rate")

  def getSampleRate: Double = $(sampleRate)

  final val numTrees = new IntParam(this, "numTrees", "number of trees to build")

  def getNumTrees: Int = $(numTrees)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    schema
  }
}

trait ANN1ModelParams extends Params {

  final val trainVal: Param[String] = new Param[String](this, "trainVal", "train value")

  def getTrainVal: String = $(trainVal)

  final val testVal: Param[String] = new Param[String](this, "testVal", "test value")

  def getTestVal: String = $(testVal)

  final val targetCol: Param[String] = new Param[String](this, "targetCol", "target column name")

  def getTargetCol: String = $(targetCol)

  final val k: Param[Int] = new IntParam(this, "k", "number of neighbors to query")

  def getK: Int = $(k)

}

class ANN1Model private[ml] (
  override val uid: String,
  val index: Index,
  @transient val items: Dataset[IdVector]
)
  extends Model[ANN1Model] with ANN1Params with ANN1ModelParams with MLWritable {

  setDefault(trainVal -> "train",  testVal -> "test", targetCol -> "target")

  override def copy(extra: ParamMap): ANN1Model = {
    copyValues(new ANN1Model(uid, index, items), extra)
  }

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setTargetCol(value: String): this.type = set(targetCol, value)

  def setTrainVal(value: String): this.type = set(trainVal, value)

  def setTestVal(value: String): this.type = set(testVal, value)

  def setK(value: Int): this.type = set(k, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val sparkSession = dataset.sparkSession
    import sparkSession.implicits._

    transformSchema(dataset.schema, logging = true)

    val instance = dataset.select(col($(idCol)), col($(featuresCol))).rdd.map {
      case Row(id: Int, point: Vector) => IdVectorWithNorm(id, point)
    }

    val bcIndex = sparkSession.sparkContext.broadcast(index)
    val candidates = instance.map { point =>
      point -> bcIndex.value.getCandidates(point)
    }

    import org.apache.spark.mllib.rdd.MLPairRDDFunctions._

    val nns = candidates.cartesian(items.as[IdVectorWithNorm].rdd)
      .filter { case ((_, c), t) => c.contains(t.id) }
      .map { case ((point, _), t) =>
        point.id -> (t.id, CosineTree.cosineDistance(point, t))
      }
      .topByKey(100)(Ordering.by(-_._2))

    nns.toDF("id", "nns")
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def write: MLWriter = new ANN1Model.ANN1ModelWriter(this)

}

object ANN1Model extends MLReadable[ANN1Model] {

  override def read: MLReader[ANN1Model] = new ANN1ModelReader

  override def load(path: String): ANN1Model = super.load(path)

  private[ANN1Model] class ANN1ModelWriter(instance: ANN1Model) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val indexPath = new Path(path, "index").toString
      val itemPath = new Path(path, "items").toString
      val data = instance.index.copyStructuredForest()
      sparkSession.createDataFrame(Array(data)).repartition(1).write.parquet(indexPath)
      instance.items.write.parquet(itemPath)
    }
  }

  private class ANN1ModelReader extends MLReader[ANN1Model] {

    /** Checked against metadata when loading model */
    private val className = classOf[ANN1Model].getName

    override def load(path: String): ANN1Model = {
      val sparkSession = super.sparkSession
      import sparkSession.implicits._
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val treePath = new Path(path, "index").toString
      val itemPath = new Path(path, "items").toString
      val forest = sparkSession.read.parquet(treePath).as[StructuredForest].head()
      val items = sparkSession.read.parquet(itemPath).as[IdVector]
      val model = new ANN1Model(metadata.uid, forest.copyCosineForest(), items)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

class ANN1(override val uid: String)
  extends Estimator[ANN1Model] with ANN1Params with DefaultParamsWritable {

  setDefault(idCol -> "id", steps -> 29, l -> 10000, sampleRate -> 0, numTrees -> 1)

  override def copy(extra: ParamMap): ANN1 = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("ann"))

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setSteps(value: Int): this.type = set(steps, value)

  def setL(value: Int): this.type = set(l, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  def setNumTrees(value: Int): this.type = set(numTrees, value)

  override def fit(dataset: Dataset[_]): ANN1Model = {
    val sparkSession = dataset.sparkSession
    import sparkSession.implicits._
    transformSchema(dataset.schema, logging = true)

    val fraction = 0.01
    val leafNodeCapacity = 25 + 2

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    val instances = dataset.as[IdVector].map {
      case IdVector(id, vector) => IdVectorWithNorm(id, vector)
    }

    if (handlePersistence) {
      instances.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val globalAggregator = new IndexAggregator

    0 until $(numTrees) foreach { _ =>
      val localIndexBuilder = new IndexBuilder(1, leafNodeCapacity)(new Random)
      val samples = instances.sample(fraction).collect()
      val nodes = localIndexBuilder.build(samples)
      val masterIndex = new Index(nodes.toArray, false)
      val bcIndex = sparkSession.sparkContext.broadcast(masterIndex)

      val distNodes = instances.rdd
        .mapPartitions { it =>
          implicit val random: Random = new Random()
          it map { v => bcIndex.value.traverse(v.vector) -> v }
        }
        .groupByKey()
        .map { case (subTreeId, it) =>
          // subTreeId is the Id of LeafNode or FlipNode of masterIndex.
          // It will be replaced with the subtree built on Spark nodes.

          val subInstances = it.toIndexedSeq

          if (subInstances.length < leafNodeCapacity) {
            subTreeId -> IndexedSeq(LeafNode(subInstances.map(_.id).toArray))
          } else {
            val builder = new IndexBuilder(1, leafNodeCapacity)(new Random())
            subTreeId -> builder.build(subInstances)
          }
        }

      distNodes.persist(StorageLevel.MEMORY_AND_DISK)
      distNodes.count() // materialize

      val aggregator = new IndexAggregator().aggregate(masterIndex.nodes)
      distNodes.toLocalIterator foreach {
        case (subTreeId, subTreeNodes) => aggregator.mergeSubTree(subTreeId, subTreeNodes)
      }
      distNodes.unpersist()
      bcIndex.destroy()

      globalAggregator.aggregate(aggregator.nodes)
    }

    if (handlePersistence) {
      instances.unpersist()
    }

    val model = copyValues(new ANN1Model(uid, globalAggregator.result(), dataset.as[IdVector])).setParent(this)
    //instr.logSuccess(model)
    model
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object ANN1 extends DefaultParamsReadable[ANN1] {
  override def load(path: String): ANN1 = super.load(path)
}
