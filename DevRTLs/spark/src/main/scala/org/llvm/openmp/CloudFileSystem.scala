package org.llvm.openmp

import java.io.InputStream
import java.io.OutputStream
import java.util.HashMap

import scala.util.Try

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.CompressionCodec

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

class CloudFileSystem(fs: FileSystem, path: String, compressOption: String) {

  val MIN_SIZE_COMPRESSION = 1000000

  val ccf = new CompressionCodecFactory(new Configuration)
  
  var compress = false
  var codec : CompressionCodec = null
  
  compressOption match {
    case "gzip" =>
      compress = true
      codec = ccf.getCodecByName(compressOption)
    case "lz4" =>
      compress = true
      codec = ccf.getCodecByName(compressOption)
    case _ =>
      compress = false
  } 

  def write(name: Integer, size: Integer, data: Array[Byte]): Unit = {
    val compressIt = compress && size >= MIN_SIZE_COMPRESSION
    val filepath = new Path(path + name)
    if (data.size != size)
      throw new RuntimeException("Wrong output size of " +
        filepath.toString() + " : " + data.size + " instead of " + size)
    var os: OutputStream = fs.create(filepath)
    if (compressIt)
      os = codec.createOutputStream(os)
    os.write(data)
    os.close
  }

  def read(name: String): InputStream = {
    val filepath = new Path(path + name)
    fs.open(filepath)
  }

  def read(name: Integer, size: Integer): Array[Byte] = {
    val compressIt = compress && size >= MIN_SIZE_COMPRESSION
    val filepath = new Path(path + name)
    var is: InputStream = fs.open(filepath)
    if (compressIt)
      is = codec.createInputStream(is)
    val data = IOUtils.toByteArray(is)
    is.close
    if (data.size != size)
      throw new RuntimeException("Wrong input size of " +
        filepath.toString() + " : " + data.size + " instead of " + size)

    return data
  }

}
