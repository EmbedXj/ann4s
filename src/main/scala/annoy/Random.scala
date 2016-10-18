package annoy

trait Random {
  def flip(): Boolean
  def index(n: Int): Int
}

object RandRandom extends Random {
  val rnd = new scala.util.Random
  override def flip(): Boolean = rnd.nextBoolean()
  override def index(n: Int): Int = rnd.nextInt(n)
}

object FixRandom extends Random {
  var fix = 0L
  override def flip(): Boolean = {
    val r = if ((fix & 1) == 1) true else false
    fix += 1
    r
  }
  override def index(n: Int): Int = {
    val r = (fix % n).toInt
    fix += 1
    r
  }
}
