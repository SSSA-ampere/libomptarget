package org.llvm.openmp

import java.io.InputStream
import java.util.HashMap
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3
import java.net.URI
import org.apache.hadoop.fs.s3native.NativeS3FileSystem

object AddressTable {

  def create(fs: CloudFileSystem): HashMap[Integer, Integer] = {
    // Initialize address table
    val csv = IOUtils.toString(fs.read("addressTable"))
    val mapSize = new HashMap[Integer, Integer]
    for (line <- csv.lines) {
      val values = line.split(";").map(_.trim)
      val name = values(0).toInt
      val size = values(1).toInt
      mapSize.put(name, size)
    }
    return mapSize
  }
}

class CloudFileSystem(fs: FileSystem, path: String) {
  
  def write(name: Int, data: Array[Byte]): Unit = {
    val filepath = new Path(path + name)
    val os = fs.create(filepath)
    os.write(data)
    os.close
  }

  def read(name: String): InputStream = {
    val filepath = new Path(path + name)
    fs.open(filepath)
  }

  def read(name: Integer, size: Integer): Array[Byte] = {
    val filepath = new Path(path + name)
    val is = fs.open(filepath)
    val data = IOUtils.toByteArray(is)
    is.close
    return data
  }

}
