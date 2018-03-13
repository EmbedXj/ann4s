package org.apache.spark.ml.nn

import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.BoundedPriorityQueue

import scala.util.Random

case class NN(id: Int, neighbors: Array[Int], distances: Array[Double])

case class Data(point: Vector, target: String, id: Int)

trait ANNParams extends Params with HasFeaturesCol
  with HasSeed {

  final val steps = new IntParam(this, "steps", "The number of steps" +
    "Must be > 0 and < 30.", ParamValidators.inRange(1, 29))

  def getSteps: Int = $(steps)

  final val l = new IntParam(this, "l", "max number of items in a leaf")

  def getL: Int = $(l)

  final val sampleRate = new DoubleParam(this, "sampling rate", "sampling rate")

  def getSampleRate: Double = $(sampleRate)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    schema
  }
}

trait ANNModelParams extends Params {

  final val trainVal: Param[String] = new Param[String](this, "trainVal", "train value")

  def getTrainVal: String = $(trainVal)

  final val testVal: Param[String] = new Param[String](this, "testVal", "test value")

  def getTestVal: String = $(testVal)

  final val targetCol: Param[String] = new Param[String](this, "targetCol", "target column name")

  def getTargetCol: String = $(targetCol)

  final val idCol: Param[String] = new Param[String](this, "idCol", "id column name")

  def getIdCol: String = $(idCol)

  final val k: Param[Int] = new IntParam(this, "k", "number of neighbors to query")

  def getK: Int = $(k)

}

class ANNModel private[ml] (override val uid: String, val tree: Array[TreeNode])
  extends Model[ANNModel] with ANNParams with ANNModelParams with MLWritable {

  setDefault(trainVal -> "train",  testVal -> "test", targetCol -> "target", idCol -> "id")

  override def copy(extra: ParamMap): ANNModel = {
    copyValues(new ANNModel(uid, tree), extra)
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

    val treeModel = new TreeModel(tree.map(x => x.nodeId -> x.hyperplane).toMap)

    val instances = dataset.select(col($(featuresCol)) as "point",
      col($(targetCol)) as "target", col($(idCol)) as "id").as[Data]

    val targetTrain = $(trainVal)
    val targetTest = $(testVal)
    val _k = $(k)

    val nns = instances.groupByKey(x => treeModel.traverse(x.point)).flatMapGroups { (_, it) =>
      val all = it.toArray
      val train = all.filter(_.target == targetTrain)
      val test = all.filter(_.target == targetTest)
      val trainWithNorm = train.map { t =>
        t -> Vectors.norm(t.point, 2)
      }

      test.map { q =>
        val pq = new BoundedPriorityQueue[(Int, Double)](_k)(Ordering.by(-_._2))
        val nrm2 = Vectors.norm(q.point, 2)

        trainWithNorm foreach { case (t, tnrm2) =>
          val dist = CosineTree.cosineDistance(q.point, nrm2, t.point, tnrm2)
          pq += t.id -> dist
        }
        val result = pq.toArray.sortBy(_._2)

        NN(q.id, result.map(_._1), result.map(_._2))
      }
    }

    nns.toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def write: MLWriter = new ANNModel.ANNModelWriter(this)

}

object ANNModel extends MLReadable[ANNModel] {

  override def read: MLReader[ANNModel] = new ANNModelReader

  override def load(path: String): ANNModel = super.load(path)

  private[ANNModel] class ANNModelWriter(instance: ANNModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(instance.tree).repartition(1).write.parquet(dataPath)
    }
  }

  private class ANNModelReader extends MLReader[ANNModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[ANNModel].getName

    override def load(path: String): ANNModel = {
      val sparkSession = super.sparkSession
      import sparkSession.implicits._
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val tree = sparkSession.read.parquet(dataPath).as[TreeNode].collect()
      val model = new ANNModel(metadata.uid, tree)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

class ANN(override val uid: String)
  extends Estimator[ANNModel] with ANNParams with DefaultParamsWritable {

  setDefault(steps -> 29, l -> 10000, sampleRate -> 0)

  override def copy(extra: ParamMap): ANN = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("ann"))

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setSteps(value: Int): this.type = set(steps, value)

  def setL(value: Int): this.type = set(l, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  override def fit(dataset: Dataset[_]): ANNModel = {
    transformSchema(dataset.schema, logging = true)

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    val instances = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => point
    }

    if (handlePersistence) {
      instances.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val count = instances.count()

    if ($(sampleRate) == 0) {
      set(sampleRate, math.max(10000, $(l)) / count.toDouble)
    }

    val numExpectedSamples = (count * $(sampleRate)).toLong

    val instr = Instrumentation.create(this, instances)
    instr.logParams(featuresCol, seed, steps, l, sampleRate)
    instr.logNamedValue("number of expected samples", numExpectedSamples)

    // algorithm
    val random= new Random($(seed))
    val tree = new CosineTree(count, $(steps), $(l), $(sampleRate))

    while (!tree.finished()) {
      val samples = instances.sample(withReplacement = false, $(sampleRate), random.nextLong())
      val grouped = samples.keyBy(tree.traverse).groupByKey().mapValues(_.toArray).toLocalIterator
      tree.split(grouped, instr)
    }

    val model = copyValues(new ANNModel(uid, tree.result()).setParent(this))
    instr.logSuccess(model)
    if (handlePersistence) {
      instances.unpersist()
    }
    model
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object ANN extends DefaultParamsReadable[ANN] {
  override def load(path: String): ANN = super.load(path)
}
