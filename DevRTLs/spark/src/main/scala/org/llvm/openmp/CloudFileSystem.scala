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

  val MIN_SIZE_COMPRESSION = 1 * 10 ^ 6
  var compressionCodec = "gzip"
  var fileExtension = ".gz"
  var compress = true
  
  compressOption.toLowerCase() match {
    case "gzip" =>
      compressionCodec = "gzip"
      fileExtension = ".gz"
    case "lz4" =>
      compressionCodec = "lz4"
      fileExtension = ".lz4"
    case "none" =>
      compress = false
    case "false" =>
      compress = false
    case "true" =>
      // keep default setting
    case _ => throw new RuntimeException(compressOption + " is not a supported compression format.")
  }

  val ccf = new CompressionCodecFactory(new Configuration)
  val codec = ccf.getCodecByName(compressionCodec)

  def write(name: Integer, size: Integer, data: Array[Byte]): Unit = {
    val compressIt = compress && size >= MIN_SIZE_COMPRESSION
    val filepath = path + name + (if (compressIt) fileExtension else "")
    var os: OutputStream = fs.create(new Path(filepath))
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
    val filepath = path + name + (if (compressIt) fileExtension else "")
    var is: InputStream = fs.open(new Path(filepath))
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
