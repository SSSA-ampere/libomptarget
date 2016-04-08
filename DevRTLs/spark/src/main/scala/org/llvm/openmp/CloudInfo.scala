package org.llvm.openmp

import java.io.InputStream
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
import java.util.HashMap
import java.util.Arrays

abstract class CloudFileSystem(val name: String) {

  def write(name: Integer, data: Array[Byte]): Unit
  
  def read(name: String): InputStream
  
  def read(name: Integer, size: Integer): Array[Byte] = {
    val is = read(name.toString)
    val data = IOUtils.toByteArray(is)
    is.close
    return data
  }

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
  
  override def read(name: String): InputStream = {
    var key = path + name
    if (key.startsWith("/"))
      key = key.substring(1)
    amazonS3Client.getObject(bucket, key).getObjectContent()
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
  
  override def read(name: String): InputStream = {
    val filepath = new Path(path + name)
    fs.open(filepath)
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

class AddressTable(csv : String) {
  val mapSize = new HashMap[Integer, Integer]
  
  for (line <- csv.lines) {
    val values = line.split(";").map(_.trim)
    val name = values(0).toInt
    val size = values(1).toInt
    mapSize.put(name, size)
  }
  
  def sizeOf(name : Integer) = {
    mapSize.get(name)
  }
}

class CloudInfo(var filesystem: String, var uri: String, var username: String, var path: String) {

  val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(conf)

  // Initialize file system according to the provider
  val fs = filesystem match {
    case "S3" => new S3(path, uri)
    case "HDFS" => new Hdfs(path, uri, username)
    case _ => throw new RuntimeException(filesystem + " is not a supported file system.")
  }
  
  // Initialize address table
  val csv = fs.read("addressTable")
  val addressTable = new AddressTable(IOUtils.toString(csv))
  
  // Load library containing native kernel
  sc.addFile(fullpath + NativeKernels.LibraryName)

  def writeRDD(name: Integer, data: RDD[Array[Byte]]): Unit = {
    data.saveAsObjectFile(fullpath + name)
  }

  def indexedWrite(name: Integer, elementSize: Integer, data: RDD[(Long, Array[Byte])]): Unit = {
    val totalSize = addressTable.sizeOf(name)
    val bytes = Array.fill[Byte](totalSize)(0)
    data.collect.foreach(x => Array.copy(x._2, 0, bytes, x._1.toInt*elementSize, elementSize))
    fs.write(name, bytes)
  }

  def readRDD(id: Integer, size: Integer): RDD[Array[Byte]] = {
    sc.binaryRecords(fullpath + id, size)
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

  def fullpath = {
    uri + path
  }

}
