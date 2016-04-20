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
    val bx = ByteBuffer.wrap(x).array()
    val by = ByteBuffer.wrap(y).array()
    for(i <- 0 to bx.size-1) {
      bx(i) = (bx(i) | by(i)).toByte
    }
    return bx
  }
  
  def bitor2(x: Array[Byte], y: Array[Byte]) : Array[Byte] = {
    (x.toStream zip y.toStream).map{ case (a,b) => (a|b).toByte }.toArray
  }
  
  def bitor3(x: Array[Byte], y: Array[Byte]) : Array[Byte] = {
    val bx = ByteBuffer.wrap(x).asIntBuffer().array()
    val by = ByteBuffer.wrap(y).asIntBuffer().array()
    val hi = (bx.toStream zip by.toStream).map{ case (a,b) => (a|b) }.toArray
    return x
  }

}