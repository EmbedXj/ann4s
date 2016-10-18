package annoy

import org.scalatest.{FlatSpec, Matchers}


class AnnoySpec extends FlatSpec with Matchers {

  "Default" should "work" in {
    val f = 10
    val r = new scala.util.Random(0)
    val annoy = new AnnoyIndex(f, FixRandom, "db")
    (0 until 1000) foreach { i =>
      val w = Array.fill(f)(r.nextGaussian().toFloat)
      annoy.addItem(s"$i", w, s"""{"id": $i}""")
    }
    // ({"id": 739},0.4293379) ({"id": 510},0.6454089) ({"id": 279},0.66716933) ({"id": 68},0.66854465) ({"id": 130},0.73778325) ({"id": 799},0.7584828) ({"id": 8},0.7848469) ({"id": 733},0.8100814) ({"id": 554},0.81120265) ({"id": 394},0.8204029)
//    annoy.cleanupTrees()
    annoy.addTrees(10)
    val w = Array.fill(f)(r.nextGaussian().toFloat)
    println(annoy.query(w, 10).mkString(" "))
    annoy.close()
  }

}

