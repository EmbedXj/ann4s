package ann4s.spark.distributed

case class CompactVector(data: Array[Byte], w: Float, b: Float) {

  def d: Int = data.length

  def recon(): Array[Float] = {
    val dst = new Array[Float](data.length)
    recon(dst)
    dst
  }

  def recon(dst: Array[Float]): Unit = {
    var i = 0
    while (i < data.length) {
      dst(i) = getFloat(i)
      i += 1
    }
  }

  def diff(ar: Array[Float]): Float = {
    assert(ar.length == data.length)
    var err = 0.0
    var i = 0
    while (i < data.length) {
      err += math.pow(ar(i) - getFloat(i), 2)
      i += 1
    }
    math.sqrt(err).toFloat
  }

  def getFloat(i: Int): Float = {
    data(i).toFloat * w + b
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
      else if (x > u) u = x
      i += 1
    }
    (l, u)
  }

  def apply(ar: Array[Float]): CompactVector = {
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

    val w = 256 / (n * (u - l))
    val b = - (256 * l) / (u - l) - 128

    val data = new Array[Byte](ar.length)
    var i = 0
    while (i < ar.length) {
      var q = math.round(w * ar(i) + b)
      q = math.max(Byte.MinValue, q)
      q = math.min(Byte.MaxValue, q)
      data(i) = q.toByte
      i += 1
    }
    CompactVector(data, 1/w, -b/w)
  }

}
