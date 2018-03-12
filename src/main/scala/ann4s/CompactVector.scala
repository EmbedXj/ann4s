package ann4s

case class CompactVector(data: Array[Byte], w: Float, b: Float, revNrm2: Float) {

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

  def cosineDistance(v: Array[Float], nrm2: Float): Float = {
    assert(data.length == v.length)
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

  def apply(ar: Array[Float], toUnit: Boolean = false): CompactVector = {
    // n = 2-norm of ar
    // l = lower bound of x / n
    // u = upper bound of x / n
    // l < x / n < u => scaling to [-128, 128]
    // 0 < x / n - l < u - l
    // 0 < (x / n - l) / (u - l) < 1
    // 0 < (x / n - l) / (u - l) * 256 < 256
    // -128 < (x / n - l) / (u - l) * 256 - 128 < 128
    //        -------------- q -----------------
    // w = 256 / (n * (u - l))
    // b = - (256 * l) / (u - l) - 128
    // q = w * x + b.

    // x = (q - b) / w
    // x = q/w - b/w
    val n = getNrm2(ar)
    val (l, u) = getUnitMinMax(ar, n)
    if (math.abs(l - u) < 1e-8) {
      CompactVector(new Array[Byte](ar.length), 0, l, if (toUnit) 1 else 1 / n)
    } else {
      val w = 256 / (n * (u - l))
      val b = -(256 * l) / (u - l) - 128

      val data = new Array[Byte](ar.length)
      var i = 0
      while (i < ar.length) {
        var q = math.round(w * ar(i) + b)
        q = math.max(Byte.MinValue, q)
        q = math.min(Byte.MaxValue, q)
        data(i) = q.toByte
        i += 1
      }
      CompactVector(data, 1 / w, -b / w, if (toUnit) 1 else 1 / n)
    }
  }

}
