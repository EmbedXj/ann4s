package ann4s

import java.nio.ByteBuffer

import ann4s.legacy._
import org.scalatest._
import org.scalatest.Matchers._

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class CreateSplitTest extends FunSuite {

  test("twoMeans") {

    val n = 2 + Functions.iterationSteps
    val d = 64

    val samples = Array.fill(n) {
      CompactVector(Array.fill(d)(Random.nextFloat()))
    }
    // for lagacy implement
    val nodes = new ArrayBuffer[Node]()
    samples.zipWithIndex foreach { case (v, i) =>
      val node = Node(d, 0, ByteBuffer.allocate(Angular.offsetValue + d * 4), 0, Angular)
      node.setVector(v.vector())
      nodes += node
    }

    val rnd = new legacy.Random {
      var i = 0
      override def index(n: Int): Int = {val r = i; i += 1; r}
      override def flip(): Boolean = false
    }

    val h1Node = Node(d, 0, ByteBuffer.allocate(Angular.offsetValue + d * 4), 0, Angular)
    Angular.createSplit(nodes, d, rnd, h1Node)
    val h1 = h1Node.getVector(new Array[Float](d))

    val h2 = CosineTree.createSplit(samples).unitVector()

    val error = math.sqrt(h1.zip(h2).map(x => math.pow(x._1 - x._2, 2)).sum)

    error should be < math.sqrt(CompactVector.Tolerance * d)

  }


}
