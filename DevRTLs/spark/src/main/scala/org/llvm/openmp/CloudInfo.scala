package org.llvm.openmp

import java.io.ByteArrayInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata

abstract class CloudFileSystem(val name: String) {

  def write(name: Integer, data: Array[Byte]): Unit

}

class S3(path: String, uri: String) extends CloudFileSystem("S3") {
  
  val credentials = new BasicAWSCredentials(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"))
  //val credentials = new BasicAWSCredentials("AKIAI5QLH6QEQW6LSP4A", "3rQ85fkeOMlnwfCCMfkSxfUawl56dYnsRIjDbpNF")
  val amazonS3Client = new AmazonS3Client(credentials)
  val bucket = uri.stripPrefix("s3n://")

  override def write(name: Integer, data: Array[Byte]): Unit = {
    var key = path + name
    if (key.startsWith("/"))
      key = key.substring(1)
    amazonS3Client.putObject(bucket, key, new ByteArrayInputStream(data), new ObjectMetadata)
  }
}

class Hdfs(path: String, uri: String, username: String) extends CloudFileSystem("HDFS") {

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
}

class CloudInfo(var filesystem: String, var uri: String, var username: String, var path: String) {

  val conf = new SparkConf()
  val sc = new SparkContext(conf)

  val fs = filesystem match {
    case "S3" => new S3(path, uri)
    case "HDFS" => new Hdfs(path, uri, username)
    case _ => throw new RuntimeException(filesystem + " is not a supported file system.")
  }

  // Load library containing native kernel
  sc.addFile(fullpath + "libmr.so")

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
    sc.binaryRecords(fullpath + id, size)
  }

  def indexedRead(id: Integer, size: Integer): RDD[(Long, Array[Byte])] = {
    read(id, size).zipWithUniqueId().map { x => (x._2, x._1.clone()) }
  }

  def fullpath = {
    uri + path
  }

}
