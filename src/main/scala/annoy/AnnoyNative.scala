package annoy

import com.sun.jna._

trait AnnoyNative extends Library {
  def createAngular(f: Int): Pointer
  def createEuclidean(f: Int): Pointer
  def deleteIndex(ptr: Pointer): Unit
  def addItem(ptr: Pointer, item: Int, w: Array[Float]): Unit
  def build(ptr: Pointer, q: Int): Unit
  def save(ptr: Pointer, filename: String): Boolean
  def unload(ptr: Pointer): Unit
  def load(ptr: Pointer, filename: String): Boolean
  def getDistance(ptr: Pointer, i: Int, j: Int): Float
  def getNnsByItem(ptr: Pointer, item: Int, n: Int, searchK: Int, result: Array[Int], distances: Array[Float]): Unit
  def getNnsByVector(ptr: Pointer, w: Array[Float], n: Int, searchK: Int, result: Array[Int], distances: Array[Float]): Unit
  def getNItems(ptr: Pointer): Int
  def verbose(ptr: Pointer, v: Boolean): Unit
  def getItem(ptr: Pointer, item: Int, v: Array[Float]): Unit
}

object AnnoyNative {
  val annoyNative = Native.loadLibrary("annoy", classOf[AnnoyNative]).asInstanceOf[AnnoyNative]
}
