package org.llvm.openmp

import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles

object Util {
  
  def loadLibrary(sc: SparkContext, info: HdfsInfo) {
    sc.addFile(info.fullpath + "libmr.so")
    System.load(SparkFiles.get("libmr.so"))
  }
}