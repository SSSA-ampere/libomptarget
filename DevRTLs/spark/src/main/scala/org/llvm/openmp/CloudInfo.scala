package org.llvm.openmp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD

class CloudInfo(var uri: String, var username: String, var path: String) {

  val conf = new SparkConf() //.setAppName("HdfsTest").setMaster("local")
  val sc = new SparkContext(conf)

  // Load library containing native kernel
  sc.addFile(fullpath + "libmr.so")

  def write(name: Integer, data: Array[Byte]): Unit = {
    System.setProperty("HADOOP_USER_NAME", username)
    val filepath = new Path(path + name)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(filepath)
    os.write(data)
    fs.close()
  }

  def write(name: Integer, data: RDD[Array[Byte]]): Unit = {
    data.saveAsObjectFile(fullpath + name)
  }

  def indexedWrite(name: Integer, size: Integer, data: RDD[(Long, Array[Byte])]): Unit = {
    val zero = for (i <- 0 to size - 1) yield 0
    val test = zero.map { x => x.toByte }.toArray
    val missing = sc.parallelize(0.toLong to 299).subtract(data.keys).map { x => (x, Array[Byte](0, 0, 0, 0)) }
    val all = missing.++(data)
    fs.write(name, all.sortByKey(true).values.collect.flatten)
  }

  def read(id: Integer, size: Integer): RDD[Array[Byte]] = {
    sc.binaryRecords(uri + path + id, size)
  }
  def indexedRead(id: Integer, size: Integer): RDD[(Long, Array[Byte])] = {
    read(id, size).zipWithUniqueId().map { x => (x._2, x._1.clone()) }
  }

  def fullpath = {
    uri + path
  }

}
