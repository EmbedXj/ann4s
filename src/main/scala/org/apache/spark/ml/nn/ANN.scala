package org.apache.spark.ml.nn

import ann4s.{CompactVector, CosineTree}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml._
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

trait ANNParams extends Params with HasFeaturesCol
  with HasSeed with HasPredictionCol {

  final val steps = new IntParam(this, "steps", "The number of steps" +
    "Must be > 0 and < 30.", ParamValidators.inRange(1, 29))

  def getSteps: Int = $(steps)

  final val l = new IntParam(this, "l", "max number of items in a leaf")

  def getL: Int = $(l)

  final val sampleRate = new DoubleParam(this, "sampling rate", "sampling rate")

  def getSampleRate: Double = $(sampleRate)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}

class ANNModel private[ml] (override val uid: String, val tree: Map[BigInt, CompactVector])
  extends Model[ANNModel] with ANNParams with MLWritable {

  override def copy(extra: ParamMap): ANNModel = {
    copyValues(new ANNModel(uid, tree), extra)
  }

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val predictUDF = udf((vector: Vector) => predict(vector))
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  private[nn] def predict(features: Vector): Int = ???

  override def write: MLWriter = new ANNModel.ANNModelWriter(this)

}

object ANNModel extends MLReadable[ANNModel] {

  override def read: MLReader[ANNModel] = new ANNModelReader

  override def load(path: String): ANNModel = super.load(path)

  private case class Data(node: BigInt, hyperplane: CompactVector)

  private[ANNModel] class ANNModelWriter(instance: ANNModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data: Array[Data] = instance.tree.map { case (node, hyperplane) =>
        Data(node, hyperplane)
      }.toArray
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(data).repartition(1).write.parquet(dataPath)
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
      val tree = sparkSession.read.parquet(dataPath).as[Data].collect().map {
        case Data(nodeId, hyperplane) => nodeId -> hyperplane
      }.toMap
      val model = new ANNModel(metadata.uid, tree)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

class ANN(override val uid: String)
  extends Estimator[ANNModel] with ANNParams with DefaultParamsWritable {

  setDefault(steps -> 29, l -> 10000, sampleRate -> 0.1)

  override def copy(extra: ParamMap): ANN = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("ann"))

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setSteps(value: Int): this.type = set(steps, value)

  def setL(value: Int): this.type = set(l, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  override def fit(dataset: Dataset[_]): ANNModel = {
    transformSchema(dataset.schema, logging = true)

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    val instances = dataset.select(col($(featuresCol))).rdd.map {
      case Row(point: Vector) => CompactVector(point.toArray.map(_.toFloat))
    }

    if (handlePersistence) {
      instances.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val count = instances.count()
    val numExpectedSamples = (count * $(sampleRate)).toLong

    val instr = Instrumentation.create(this, instances)
    instr.logParams(featuresCol, predictionCol, seed, steps, l, sampleRate)
    instr.logNamedValue("number of expected samples", numExpectedSamples)

    // algorithm
    val random= new Random($(seed))
    val tree = new CosineTree(count, $(steps), $(l), $(sampleRate))

    while (!tree.finished()) {
      val samples = instances.sample(withReplacement = false, $(sampleRate), random.nextLong())
      val grouped = samples.keyBy(tree.traverse).groupByKey().mapValues(_.toArray).toLocalIterator
      tree.split(grouped, instr)
    }

    val model = copyValues(new ANNModel(uid, tree.getTree).setParent(this))
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
