package ann4s

case class CompactVector(data: Array[Short], w: Float, b: Float, revNrm2: Float) {

  def d: Int = data.length

  def unitVector(): Array[Float] = Array.tabulate(data.length)(unit)

  def vector(): Array[Float] = Array.tabulate(data.length)(apply)

  def euclideanDistance(ar: Array[Float]): Float = {
    assert(ar.length == data.length)
    var err = 0.0
    var i = 0
    while (i < data.length) {
      val xmm = ar(i) - apply(i)
      err += xmm * xmm
      i += 1
    }
    math.sqrt(err).toFloat
  }

  def cosineDistance(v: Array[Float]): Float = {
    assert(data.length == v.length)
    val nrm2 = CompactVector.getNrm2(v)
    var i = 0
    var dot = 0f
    while (i < data.length) {
      dot += unit(i) * v(i) / nrm2
      i += 1
    }
    2 - 2 * dot
  }

  def apply(i: Int): Float = {
    data(i).toFloat * w + b
  }

  def unit(i: Int): Float = {
    apply(i) * revNrm2
  }
}

object CompactVector {

  val Tolerance = 1e-5

  def getNrm2(ar: Array[Float]): Float = {
    var nrm2 = 0.0
    var i = 0
    while (i < ar.length) {
      nrm2 += ar(i) * ar(i)
      i += 1
    }
    if (nrm2 < 1e-8) 1f else math.sqrt(nrm2).toFloat
  }

  def getUnitMinMax(ar: Array[Float], nrm2: Float): (Float, Float) = {
    var l = Float.MaxValue
    var u = Float.MinValue
    var i = 0
    while (i < ar.length) {
      val x = ar(i) / nrm2
      if (x < l) l = x
      if (x > u) u = x
      i += 1
    }
    (l, u)
  }

  private val OneFullScale: Int = (Byte.MaxValue + 1) * 2
  private val OneHalfScale: Int = OneFullScale / 2

  private val TwoFullScale: Int = (Short.MaxValue + 1) * 2
  private val TwoHalfScale: Int = TwoFullScale / 2

  /*
  def apply(ar: Array[Float], toUnit: Boolean = false): CompactVector = {
    // n = 2-norm of ar
    // l = lower bound of x / n
    // u = upper bound of x / n
    // l < x / n < u => scaling to [-HalfScale, HalfScale]
    // 0 < x / n - l < u - l
    // 0 < (x / n - l) / (u - l) < 1
    // 0 < (x / n - l) / (u - l) * FullScale < FullScale
    // -HalfScale < (x / n - l) / (u - l) * FullScale - HalfScale  < HalfScale
    //        -------------- q ----------------------------------------------------------
    // w = FullScale / (n * (u - l))
    // b = - (FullScale * l) / (u - l) - HalfScale
    // q = w * x + b.

    // x = (q - b) / w
    // x = q/w - b/w
    val n = getNrm2(ar)
    val (l, u) = getUnitMinMax(ar, n)
    if (math.abs(l - u) < 1e-8) {
      if (toUnit) {
        CompactVector(new Array[Byte](ar.length), 0, l / n, 1)
      } else {
        CompactVector(new Array[Byte](ar.length), 0, l, 1 / n)
      }
    } else {
      val w = OneFullScale / (n * (u - l))
      val b = -(OneFullScale * l) / (u - l) - OneHalfScale

      val data = new Array[Byte](ar.length)
      var i = 0
      while (i < ar.length) {
        var q = math.round(w * ar(i) + b)
        q = math.max(Byte.MinValue, q)
        q = math.min(Byte.MaxValue, q)
        data(i) = q.toByte
        i += 1
      }
      if (toUnit) {
        CompactVector(data, 1 / w / n, -b / w / n, 1)
      } else {
        CompactVector(data, 1 / w, -b / w, 1 / n)
      }
    }
  }
  */

  def apply(ar: Array[Float], toUnit: Boolean = false): CompactVector = {
    val n = getNrm2(ar)
    val (l, u) = getUnitMinMax(ar, n)
    if (math.abs(l - u) < 1e-8) {
      if (toUnit) {
        CompactVector(new Array[Short](ar.length), 0, l / n, 1)
      } else {
        CompactVector(new Array[Short](ar.length), 0, l, 1 / n)
      }
    } else {
      val w = TwoFullScale / (n * (u - l))
      val b = -(TwoFullScale * l) / (u - l) - TwoHalfScale

      val data = new Array[Short](ar.length)
      var i = 0
      while (i < ar.length) {
        var q = math.round(w * ar(i) + b)
        q = math.max(Short.MinValue, q)
        q = math.min(Short.MaxValue, q)
        data(i) = q.toShort
        i += 1
      }
      if (toUnit) {
        CompactVector(data, 1 / w / n, -b / w / n, 1)
      } else {
        CompactVector(data, 1 / w, -b / w, 1 / n)
      }
    }
  }

}
