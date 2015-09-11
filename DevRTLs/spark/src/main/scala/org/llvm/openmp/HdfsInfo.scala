package org.llvm.openmp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class HdfsInfo(var uri: String, var username: String, var path: String) {

  def write(name: String, data: Array[Byte]): Unit = {
    System.setProperty("HADOOP_USER_NAME", username)
    val filepath = new Path(path + name)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(filepath)
    os.write(data)
    fs.close()
  }
  
  def fullpath = {
    uri + path
  }
  
}