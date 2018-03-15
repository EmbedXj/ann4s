package org.apache.spark.ml.nn

import scala.collection.mutable.ArrayBuffer

class IndexAggregator() {

  var nodes = new ArrayBuffer[Node]()

  var withItems = false

  def aggregate(other: IndexedSeq[Node]): this.type = {
    val roots = new ArrayBuffer[Node]()
    var i = nodes.length - 1
    while (0 <= i && nodes(i).isInstanceOf[RootNode]) {
      roots.insert(0, nodes(i))
      i -= 1
    }
    // remove roots in nodes
    nodes.reduceToSize(nodes.length - roots.length)
    nodes.sizeHint(nodes.length + other.length + roots.length)

    val offset = nodes.length
    other foreach {
      case root: RootNode =>
        roots += root.copy(root.location + offset)
      case hyperplane: HyperplaneNode =>
        nodes += hyperplane.copy(l = hyperplane.l + offset, r = hyperplane.r + offset)
      case leaf: LeafNode =>
        nodes += leaf
      case _: ItemNode =>
        assert(assertion = false, "item nodes could not be aggregated")
    }
    nodes ++= roots
    this
  }

  def prependItems(items: IndexedSeq[IdVector]): this.type = {
    assert(!withItems, "items already prepended")
    withItems = true
    val itemSize = items.reduceLeft((x, y) => if (x.id > y.id) x else y).id + 1
    val itemNodes = new Array[Node](itemSize)
    for (item <- items) itemNodes(item.id) = ItemNode(item.vector)
    val oldNodes = nodes
    nodes = new ArrayBuffer[Node](itemSize + oldNodes.length)
    nodes ++= itemNodes
    aggregate(oldNodes)
  }

  // TODO: merge
  def prependItems2(items: IndexedSeq[IdVectorWithNorm]): this.type = {
    assert(!withItems, "items already prepended")
    withItems = true
    val itemSize = items.reduceLeft((x, y) => if (x.id > y.id) x else y).id + 1
    val itemNodes = new Array[Node](itemSize)
    for (item <- items) itemNodes(item.id) = ItemNode(item.vector)
    val oldNodes = nodes
    nodes = new ArrayBuffer[Node](itemSize + oldNodes.length)
    nodes ++= itemNodes
    aggregate(oldNodes)
  }

  def result(): Index = new Index(nodes.toArray, withItems)

}
