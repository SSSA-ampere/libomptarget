package org.llvm.openmp

import java.io.InputStream
import java.io.OutputStream
import java.util.HashMap
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3
import java.net.URI
import org.apache.hadoop.fs.s3native.NativeS3FileSystem
import scala.util.Try
import org.apache.hadoop.conf.Configuration
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
  val compress = Try(compressOption.toBoolean).getOrElse(true)

  val ccf = new CompressionCodecFactory(new Configuration)
  val codec = ccf.getCodecByName("gzip");

  def write(name: Integer, size: Integer, data: Array[Byte]): Unit = {
    var extension = ""
    if (compress && size >= MIN_SIZE_COMPRESSION) {
      extension = ".gz"
    }
    val filepath = new Path(path + name + extension)
    var os: OutputStream = fs.create(filepath)
    if (compress && size >= MIN_SIZE_COMPRESSION) {
      os = codec.createOutputStream(os)
    }
    os.write(data)
    os.close
  }

  def read(name: String): InputStream = {
    val filepath = new Path(path + name)
    fs.open(filepath)
  }

  def read(name: Integer, size: Integer): Array[Byte] = {
    var extension = ""
    if (compress && size >= MIN_SIZE_COMPRESSION) {
      extension = ".gz"
    }
    val filepath = new Path(path + name + extension)

    var is: InputStream = fs.open(filepath)
    if (compress && size >= MIN_SIZE_COMPRESSION) {
      is = codec.createInputStream(is)
    }
    val data = IOUtils.toByteArray(is)
    is.close
    if (data.size != size)
      throw new RuntimeException("Wrong input size of " +
        filepath.toString() + " : " + data.size + " instead of " + size)

    return data
  }

}
