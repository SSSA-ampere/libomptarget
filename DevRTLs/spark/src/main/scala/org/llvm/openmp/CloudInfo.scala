package org.llvm.openmp

import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles

object NativeKernels {

  val LibraryName = "libmr.so"

  @transient
  var isAlreadyLoaded = false

  def loadOnce(): Unit = {
    if (isAlreadyLoaded) return
    System.load(SparkFiles.get("libmr.so"))
    isAlreadyLoaded = true
  }
}

class CloudInfo(fs: CloudFileSystem, addressTable: HashMap[Integer, Integer]) {

  val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[CloudFileSystem], classOf[CloudInfo]))
    .set("spark.kryoserializer.buffer.max", "2047m")
    .set("spark.driver.maxResultSize", "0")
    
  val sc = new SparkContext(conf)
 
  // Load library containing native kernel
  sc.addFile(fs.fullpath + NativeKernels.LibraryName)

}
