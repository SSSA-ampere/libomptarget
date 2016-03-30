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
import org.apache.spark.SparkFiles
import com.amazonaws.util.IOUtils

abstract class CloudFileSystem(val name: String) {

  def write(name: Integer, data: Array[Byte]): Unit
  
  def read(name: Integer, size: Integer): Array[Byte]

}

class S3(path: String, uri: String) extends CloudFileSystem("S3") {
  
  val credentials = new BasicAWSCredentials(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"))
  val amazonS3Client = new AmazonS3Client(credentials)
  val bucket = uri.stripPrefix("s3n://")

  override def write(name: Integer, data: Array[Byte]): Unit = {
    var key = path + name
    if (key.startsWith("/"))
      key = key.substring(1)
    amazonS3Client.putObject(bucket, key, new ByteArrayInputStream(data), new ObjectMetadata)
  }
  
  override def read(name: Integer, size: Integer): Array[Byte] = {
    var key = path + name
    if (key.startsWith("/"))
      key = key.substring(1)
    val is = amazonS3Client.getObject(bucket, key).getObjectContent()
    val data = IOUtils.toByteArray(is)
    is.close
    return data
  }
  
}

class Hdfs(path: String, uri: String, username: String) extends CloudFileSystem("HDFS") {
  
  System.setProperty("HADOOP_USER_NAME", username)
  val conf = new Configuration()
  conf.set("fs.defaultFS", uri)
  val fs = FileSystem.get(conf)

  override def write(name: Integer, data: Array[Byte]): Unit = {
    val filepath = new Path(path + name)
    val os = fs.create(filepath)
    os.write(data)
    os.close
  }
  
  override def read(name: Integer, size: Integer): Array[Byte] = {
    val filepath = new Path(path + name)
    val is = fs.open(filepath, size)
    val data = IOUtils.toByteArray(is)
    is.close
    return data
  }
}

object NativeKernels {
  
  val LibraryName = "libmr.so"

  @transient 
  var isAlreadyLoaded = false

  def loadOnce() : Unit = {
    if (isAlreadyLoaded) return
    System.load(SparkFiles.get(LibraryName))
    isAlreadyLoaded = true
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
  sc.addFile(fullpath + NativeKernels.LibraryName)

  def writeRDD(name: Integer, data: RDD[Array[Byte]]): Unit = {
    data.saveAsObjectFile(fullpath + name)
  }

  def indexedWrite(name: Integer, size: Integer, data: RDD[(Long, Array[Byte])]): Unit = {
    val zero = for (i <- 0 to size - 1) yield 0
    val test = zero.map { x => x.toByte }.toArray
    val missing = sc.parallelize(0.toLong to 299).subtract(data.keys).map { x => (x, Array[Byte](0, 0, 0, 0)) }
    val all = missing.++(data)
    fs.write(name, all.sortByKey(true).values.collect.flatten)
  }

  def readRDD(id: Integer, size: Integer): RDD[Array[Byte]] = {
    sc.binaryRecords(fullpath + id, size)
  }

  def indexedRead(id: Integer, size: Integer): RDD[(Long, Array[Byte])] = {
    readRDD(id, size).zipWithUniqueId().map { x => (x._2, x._1.clone()) }
  }
  
  def read(id: Integer, size: Integer): Array[Byte] = {
    fs.read(id, size)
  }
  
  def write(name: Integer, size: Integer, data: Array[Byte]): Unit = {
    fs.write(name, data)
  }

  def fullpath = {
    uri + path
  }

}
