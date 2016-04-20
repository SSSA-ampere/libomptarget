package org.llvm.openmp

import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD

object NativeKernels {

  val LibraryName = "libmr.so"

  @transient
  var isAlreadyLoaded = false

  def loadOnce(): Unit = {
    if (isAlreadyLoaded) return
    System.load(SparkFiles.get(LibraryName))
    isAlreadyLoaded = true
  }
}

class CloudInfo(fs: CloudFileSystem, addressTable: HashMap[Integer, Integer]) {

  val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(classOf[CloudFileSystem], classOf[CloudInfo]))
  val sc = new SparkContext(conf)

  // Load library containing native kernel
  sc.addFile(fs.fullpath + NativeKernels.LibraryName)

  def writeRDD(name: Integer, data: RDD[Array[Byte]]): Unit = {
    data.saveAsObjectFile(fs.fullpath + name)
  }

  def indexedWrite(name: Integer, elementSize: Integer, data: RDD[(Long, Array[Byte])]): Unit = {
    val totalSize = addressTable.get(name)
    val bytes = Array.fill[Byte](totalSize)(0)
    data.collect.foreach(x => Array.copy(x._2, 0, bytes, x._1.toInt * elementSize, elementSize))
    fs.write(name, bytes)
  }

  def readRDD(id: Integer, size: Integer): RDD[Array[Byte]] = {
    sc.binaryRecords(fs.fullpath + id, size)
  }

  def indexedRead(id: Integer, size: Integer): RDD[(Long, Array[Byte])] = {
    readRDD(id, size).zipWithIndex().map { x => (x._2, x._1.clone()) }
  }

  def read(id: Integer, size: Integer): Array[Byte] = {
    fs.read(id, size)
  }

  def write(name: Integer, size: Integer, data: Array[Byte]): Unit = {
    fs.write(name, data)
  }



}
