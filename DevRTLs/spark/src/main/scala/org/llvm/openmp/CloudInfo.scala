package org.llvm.openmp

import java.net.URI
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import java.io.File

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

class CloudInfo(args: Array[String]) {

  val filesystem = args(0)
  val uri = args(1)
  val username = args(2)
  val path = args(3)
  
  val conf = new SparkConf()
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.registerKryoClasses(Array(classOf[CloudFileSystem], classOf[CloudInfo]))
    //.set("spark.kryoserializer.buffer.max", "2047m")
    .set("spark.driver.maxResultSize", "0")
  
  val session = SparkSession.builder().config(conf).getOrCreate()
  
  val sc = session.sparkContext
  
  filesystem match {
    case "S3" =>
      sc.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
      sc.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", args(4))
      sc.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", args(5))
      
      //sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      //sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKey)
      //sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretKey)
      
      //sc.hadoopConfiguration.set("fs.defaultFS", uri)
    case "HDFS" => 
      System.setProperty("HADOOP_USER_NAME", username)
      //sc.hadoopConfiguration.set("fs.defaultFS", uri)
    case _ => throw new RuntimeException(filesystem + " is not a supported file system.")
  }
  
  val sqlContext = new SQLContext(sc)
  
  val fs = FileSystem.get(URI.create(uri), sc.hadoopConfiguration)
  
  val localLibrary = new File("/tmp/", NativeKernels.LibraryName)
  
  FileUtil.copy(fs, new Path(uri + path + NativeKernels.LibraryName), localLibrary, false, sc.hadoopConfiguration)

  def init(fs: CloudFileSystem) {
    // Load library containing native kernel
    sc.addFile(localLibrary.getAbsolutePath)
  }
  
  def getExecutorNumber(): Integer = {
    var nb = sc.getExecutorStorageStatus.length
    if(nb > 1) nb = nb - 1 // Do not count the driver node
    return nb
  }

}
