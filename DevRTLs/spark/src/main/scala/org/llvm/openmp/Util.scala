package org.llvm.openmp

object Util {

  def bitor(x: Array[Byte], y: Array[Byte]): Array[Byte] = {
    var i = 0
    while (i < x.length) {
      x(i) = (x(i) | y(i)).toByte
      i += 1
    }
    return x
  }

}