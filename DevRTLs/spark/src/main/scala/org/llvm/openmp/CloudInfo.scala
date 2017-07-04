package org.llvm.openmp

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

import com.google.common.io.Files

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

  val conf = new SparkConf().set("spark.driver.maxResultSize", "0")

  val session = SparkSession.builder().config(conf).getOrCreate()

  val sc = session.sparkContext

  var fsConf = new Configuration

  filesystem match {
    case "S3" =>
      fsConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      fsConf.set("fs.s3a.awsAccessKeyId", args(4))
      fsConf.set("fs.s3a.awsSecretAccessKey", args(5))
    case "HDFS" =>
      System.setProperty("HADOOP_USER_NAME", username)
    case "FILE" =>
    case "" =>
    case _ => throw new RuntimeException(filesystem + " is not a supported file system.")
    
  }

  val sqlContext = new SQLContext(sc)

  val fs = FileSystem.get(URI.create(uri), sc.hadoopConfiguration)

  val myTempDir = Files.createTempDir()
  val localLibrary = new File(myTempDir, NativeKernels.LibraryName)

  FileUtil.copy(fs, new Path(uri + path + NativeKernels.LibraryName), localLibrary, false, fsConf)

  def init(fs: CloudFileSystem) {
    // Load library containing native kernel
    sc.addFile(localLibrary.getAbsolutePath)
  }

  def getExecutorNumber(): Integer = {
    var nb = sc.getExecutorStorageStatus.length
    if (nb > 1) nb = nb - 1 // Do not count the driver node
    return nb
  }

  def getParallelism(): Integer = {
    val parallelism = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    return parallelism
  }

}
