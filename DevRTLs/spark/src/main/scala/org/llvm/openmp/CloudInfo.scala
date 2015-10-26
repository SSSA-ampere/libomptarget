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

  def read(id: Integer, size: Integer): RDD[Array[Byte]] = {
    sc.binaryRecords(uri + path + id, 4)
  }

  def fullpath = {
    uri + path
  }

}
