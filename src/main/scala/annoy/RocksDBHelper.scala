package annoy

import java.io.File
import java.util

import org.rocksdb._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object RocksDBHelper {

  def using[A, B <: { def close(): Unit }](closeable: B)(f: B => A): A = {
    try {
      f(closeable)
    } finally {
      if (closeable != null) {
        closeable.close()
      }
    }
  }

}

class RocksDBHelper(dbPath: String) {


  import RocksDBHelper._

  RocksDB.loadLibrary()

  private val logPath = "/tmp/annoy/logs"

  private val writeBufferSize = 1024 * 1024 * 512

  private val arenaBlockSize = 2014 *32

  private val dbOptions = new DBOptions()
    .createStatistics()
    .setDbLogDir(logPath)
    .setTableCacheNumshardbits(5)
    .setIncreaseParallelism(8)
    .setAllowOsBuffer(true)
    .setAllowMmapReads(true)

  private val readOptions = new ReadOptions().setFillCache(true).setVerifyChecksums(false)

  private val writeOptions = new WriteOptions()

  private val lastAtomicIndexKey = "lastAtomicIndex".getBytes

  private val lastAtomicNodeIndexKey = "lastAtomicNodeIndex".getBytes

  private val columnFamilies = Seq(
    //default
    ("default", "max"),
    //atomicIndex: Int -> feat: Array[Float]
    ("item", null),
    // atomicIndex: Int -> metadata: String
    ("metadata", null),
    // id: String -> atomicIndex: Int
    ("idmap", null),
    // atomicIndex: Int -> treeNode: TreeNode
    ("node", null)
  )

  val columnFamilyDescriptors: Seq[ColumnFamilyDescriptor] = columnFamilies.map {
    case (name, mergeOperatorName) if mergeOperatorName != null =>
      new ColumnFamilyDescriptor(name.getBytes, new ColumnFamilyOptions().setMergeOperatorName(mergeOperatorName).setWriteBufferSize(writeBufferSize).setArenaBlockSize(arenaBlockSize))
    case (name, mergeOperatorName) =>
      new ColumnFamilyDescriptor(name.getBytes, new ColumnFamilyOptions().setWriteBufferSize(writeBufferSize).setArenaBlockSize(arenaBlockSize))
  }

  initializeDB()

  private val (db, handles) = openDB()
  private val defaultHandle = handles("default")
  private val itemHandle = handles("item")
  private val metadataHandle = handles("metadata")
  private val idmapHandle = handles("idmap")
  private var nodeHandle = handles("node")

  def initializeDB(): Unit = {
    if (! new File(dbPath).exists()) {
      println("initializing db ...")
      using(RocksDB.open(new Options().setCreateIfMissing(true), dbPath)) { db =>
        columnFamilyDescriptors
          .filter(d => new String(d.columnFamilyName()) != "default")
          .foreach { d =>
            println("    creating columnFamily:" + new String(d.columnFamilyName()))
            using(db.createColumnFamily(d)) { _ => }
          }
        using(db.getDefaultColumnFamily) { handle =>
          db.put(handle, lastAtomicIndexKey, Bytes.int2bytes(0))
          db.put(handle, lastAtomicNodeIndexKey, Bytes.int2bytes(0))
        }
        println("done")
      }
    }
  }

  def openDB(): (RocksDB, Map[String, ColumnFamilyHandle]) = {
    val handles = new util.ArrayList[ColumnFamilyHandle]()
    val db = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, handles)
    (db, columnFamilyDescriptors.zip(handles).map { case (key, value) => (new String(key.columnFamilyName()), value) }.toMap)
  }

  def getLastAtomicIndex: Int = {
    Bytes.bytes2int(db.get(defaultHandle, lastAtomicIndexKey), 0)
  }

  def getLastAtomicNodeIndex: Int = {
    Bytes.bytes2int(db.get(defaultHandle, lastAtomicNodeIndexKey), 0)
  }

  def putLastAtomicNodeIndex(lastAtomicNodeIndex: Int): Unit = {
    db.put(defaultHandle, writeOptions, lastAtomicNodeIndexKey, Bytes.int2bytes(lastAtomicNodeIndex))
  }

  def mergeLastAtomicNodeIndex(atomicNodeIndex: Int): Int = {
    db.merge(defaultHandle, writeOptions, lastAtomicNodeIndexKey, Bytes.int2bytes(atomicNodeIndex))
    getLastAtomicNodeIndex
  }

  def exists(id: String): Boolean = {
    db.get(idmapHandle, id.getBytes) != null
  }

  def getAtomicIndex(id: String): Int = {
    val bytes = db.get(idmapHandle, id.getBytes)
    if (bytes == null) {
      -1
    } else {
      Bytes.bytes2int(bytes, 0)
    }
  }

  def putAll(id: String, atomicIndex: Int, feat: Array[Float], metadata: String): Unit = {
    val batch = new WriteBatch()
    val atomicIndexBytes = Bytes.int2bytes(atomicIndex)
    batch.merge(defaultHandle, lastAtomicIndexKey, Bytes.int2bytes(atomicIndex + 1))
    batch.put(idmapHandle, id.getBytes, atomicIndexBytes)
    batch.put(itemHandle, atomicIndexBytes, Bytes.floats2bytes(feat))
    batch.put(metadataHandle, atomicIndexBytes, metadata.getBytes)
    db.write(writeOptions, batch)
  }

  def getMetadata(atomicIndex: Int): String = {
    new String(db.get(metadataHandle, readOptions, Bytes.int2bytes(atomicIndex)))
  }

  def putRoot(atomicNodeIndex: Int): Unit = {
    db.put(nodeHandle, writeOptions, f"root_$atomicNodeIndex%010d".getBytes, Bytes.int2bytes(atomicNodeIndex))
  }

  def getRoots: ArrayBuffer[Int] = {
    val roots = new ArrayBuffer[Int]()
    val iter = db.newIterator(nodeHandle)
    iter.seek(f"root_${0}%010d".getBytes())
    var i = 0
    while (iter.isValid && i < Int.MaxValue) {
      val root = Bytes.bytes2int(iter.value(), 0)
      println(s"load root $root")
      roots += root
      iter.next()
      i += 1
    }
    roots
  }

  def getNode(i: Int, dim: Int): (Array[Int], Array[Float]) = {
    val node = db.get(nodeHandle, readOptions, Bytes.int2bytes(i))
    if (node(0) == 'l') {
      (Bytes.bytes2ints(node, 1, -1), null)
    } else if (node(0) == 'h') {
      (Bytes.bytes2ints(node, 1, 2), Bytes.bytes2floats(node, 1 + 4 + 4, dim))
    } else {
      throw new IllegalArgumentException
    }
  }

  def putLeafNode(atomicNodeIndex: Int, children: Array[Int]): Unit = {
    val batch = new WriteBatch()
    batch.put(nodeHandle, Bytes.int2bytes(atomicNodeIndex), Array[Byte]('l') ++ Bytes.ints2bytes(children))
    batch.merge(defaultHandle, lastAtomicNodeIndexKey, Bytes.int2bytes(atomicNodeIndex))
    db.write(writeOptions, batch)
  }

  def putHyperplaneNode(atomicNodeIndex: Int, children: Array[Int], hyperplane: Array[Float]) = {
    val batch = new WriteBatch()
    batch.put(nodeHandle, Bytes.int2bytes(atomicNodeIndex), Array[Byte]('h') ++ Bytes.ints2bytes(children) ++ Bytes.floats2bytes(hyperplane))
    batch.merge(defaultHandle, lastAtomicNodeIndexKey, Bytes.int2bytes(atomicNodeIndex))
    db.write(writeOptions, batch)
  }

  def getFeat(i: Int, feat: Array[Float]): Array[Float] = {
    val bytes = db.get(itemHandle, readOptions, Bytes.int2bytes(i))
    Bytes.bytes2floats(bytes, 0, feat)
  }

  def getFeat(i: Int, dim: Int): Array[Float] = {
    val bytes = db.get(itemHandle, readOptions, Bytes.int2bytes(i))
    Bytes.bytes2floats(bytes, 0, dim)
  }

  def cleanupNodes(): Unit = {
    db.dropColumnFamily(nodeHandle)
    nodeHandle = db.createColumnFamily(
      new ColumnFamilyDescriptor("node".getBytes, new ColumnFamilyOptions().setWriteBufferSize(writeBufferSize).setArenaBlockSize(arenaBlockSize)))
  }

  def close(): Unit = {
    handles.foreach(_._2.close())
    db.close()
  }

}
