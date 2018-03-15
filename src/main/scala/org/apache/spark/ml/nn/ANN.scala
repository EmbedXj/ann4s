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

case class IdVector(id: Int, vector: Vector)

trait ANNParams extends Params with HasFeaturesCol with HasSeed {

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

trait ANNModelParams extends Params {

  final val trainVal: Param[String] = new Param[String](this, "trainVal", "train value")

  def getTrainVal: String = $(trainVal)

  final val testVal: Param[String] = new Param[String](this, "testVal", "test value")

  def getTestVal: String = $(testVal)

  final val targetCol: Param[String] = new Param[String](this, "targetCol", "target column name")

  def getTargetCol: String = $(targetCol)

  final val k: Param[Int] = new IntParam(this, "k", "number of neighbors to query")

  def getK: Int = $(k)

}

class ANNModel private[ml] (
  override val uid: String,
  val index: Index,
  @transient val items: Dataset[IdVector]
)
  extends Model[ANNModel] with ANNParams with ANNModelParams with MLWritable {

  setDefault(trainVal -> "train",  testVal -> "test", targetCol -> "target")

  override def copy(extra: ParamMap): ANNModel = {
    copyValues(new ANNModel(uid, index, items), extra)
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

  override def write: MLWriter = new ANNModel.ANNModelWriter(this)

}

object ANNModel extends MLReadable[ANNModel] {

  override def read: MLReader[ANNModel] = new ANNModelReader

  override def load(path: String): ANNModel = super.load(path)

  private[ANNModel] class ANNModelWriter(instance: ANNModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val indexPath = new Path(path, "index").toString
      val itemPath = new Path(path, "items").toString
      val data = instance.index.copyStructuredForest()
      sparkSession.createDataFrame(Array(data)).repartition(1).write.parquet(indexPath)
      instance.items.write.parquet(itemPath)
    }
  }

  private class ANNModelReader extends MLReader[ANNModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[ANNModel].getName

    override def load(path: String): ANNModel = {
      val sparkSession = super.sparkSession
      import sparkSession.implicits._
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val treePath = new Path(path, "tree").toString
      val itemPath = new Path(path, "tree").toString
      val forest = sparkSession.read.parquet(treePath).as[StructuredForest].head()
      val items = sparkSession.read.parquet(itemPath).as[IdVector]
      val model = new ANNModel(metadata.uid, forest.copyCosineForest(), items)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

class ANN(override val uid: String)
  extends Estimator[ANNModel] with ANNParams with DefaultParamsWritable {

  setDefault(idCol -> "id", steps -> 29, l -> 10000, sampleRate -> 0, numTrees -> 1)

  override def copy(extra: ParamMap): ANN = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("ann"))

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setSteps(value: Int): this.type = set(steps, value)

  def setL(value: Int): this.type = set(l, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  def setNumTrees(value: Int): this.type = set(numTrees, value)

  override def fit(dataset: Dataset[_]): ANNModel = {
    val sparkSession = dataset.sparkSession
    import sparkSession.implicits._
    transformSchema(dataset.schema, logging = true)

    val numItemsPerPartition = 100000
    val numTrees = 10
    val leafNodeCapacity = 25

    val count = dataset.count()
    val requiredPartitions = math.ceil(count / numItemsPerPartition).toInt
    val rdd = dataset.as[IdVector].rdd.map {
      case IdVector(id, vector) => IdVectorWithNorm(id, vector)
    }
    val instances = if (rdd.getNumPartitions != requiredPartitions) {
      rdd.repartition(requiredPartitions)
    } else {
      rdd
    }

    val instr = Instrumentation.create(this, instances)
    instr.logParams(featuresCol, seed, steps, l, sampleRate)

    val indices = instances.mapPartitions { it =>
      val builder = new IndexBuilder(numTrees, leafNodeCapacity)
      // is toArray harmful?
      val index = builder.build(it.toArray)
      Iterator.single(index)
    }

    val aggregator = new IndexAggregator
    indices.toLocalIterator foreach aggregator.aggregate

    val model = copyValues(new ANNModel(uid, aggregator.result(), dataset.as[IdVector])).setParent(this)
    instr.logSuccess(model)

    model
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object ANN extends DefaultParamsReadable[ANN] {
  override def load(path: String): ANN = super.load(path)
}
