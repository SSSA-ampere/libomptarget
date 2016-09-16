package org.llvm.openmp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD
import java.nio.ByteBuffer
import java.nio.IntBuffer

object Util {
 
  // zip the RDDs into an RDD of Seq[Int]
  def makeZip[U](s: Seq[RDD[U]]): RDD[Seq[U]] = {
    if (s.length == 1)
      s.head.map(e => Seq(e))
    else {
      val others = makeZip(s.tail)
      val all = s.head.zip(others)
      all.map(elem => Seq(elem._1) ++ elem._2)
    }
  }
  
  def bitor(x: Array[Byte], y: Array[Byte]) : Array[Byte] = {
    var i = 0
    while (i < x.length) {
      x(i) = (x(i) | y(i)).toByte
      i += 1
    }
    return x
  }
  
  /*
  def bitor(x: Any, y: Any) : Any = {
    return x
  }
  
  def bitor[T <: Product](x: T, y: T) : T = {
    var i = 0
    while (i < x.productArity) {
      bitor(x.productElement(i), y.productElement(i))
      i += 1
    }
    return x
  }
  
  def bitor(x: Array[Long], y: Array[Long]) : Array[Long] = {
    var i = 0
    while (i < x.length) {
      x(i) = (x(i) | y(i))
      i += 1
    }
    return x
  }

  
  def bitor2(x: Array[Byte], y: Array[Byte]) : Array[Byte] = {
    for(i <- 0 to x.size-1) {
      x(i) = (x(i) | y(i)).toByte
    }
    return x
  }
  */

}