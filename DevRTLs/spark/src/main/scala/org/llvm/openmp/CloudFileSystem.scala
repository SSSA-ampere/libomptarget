package org.llvm.openmp

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.HashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.util.IOUtils

object AddressTable {

  def create(fs: CloudFileSystem) : HashMap[Integer, Integer] = {
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

object CloudFileSystem {
  
  // Initialize file system according to the provider
  def create(filesystem: String, uri: String, username: String, path: String) = filesystem match {
    case "S3" => new S3(path, uri)
    case "HDFS" => new Hdfs(path, uri, username)
    case _ => throw new RuntimeException(filesystem + " is not a supported file system.")
  }
  
}

abstract class CloudFileSystem(path: String, uri: String) extends Serializable {
 
  def write(name: Int, data: Array[Byte])

  def read(name: String): InputStream

  def read(name: Integer, size: Integer): Array[Byte] = {
    val is = read(name.toString)
    val data = IOUtils.toByteArray(is)
    is.close
    return data
  }
  
  val fullpath = {
    uri + path
  }

}

class S3(path: String, uri: String) extends CloudFileSystem(path, uri) {

  val credentials = new BasicAWSCredentials(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"))
  val amazonS3Client = new AmazonS3Client(credentials)
  val bucket = uri.stripPrefix("s3n://")

  override def write(name: Int, data: Array[Byte]) {
    var key = path + name
    if (key.startsWith("/"))
      key = key.substring(1)
    amazonS3Client.putObject(bucket, key, new ByteArrayInputStream(data), new ObjectMetadata)
  }

  override def read(name: String): InputStream = {
    var key = path + name
    if (key.startsWith("/"))
      key = key.substring(1)
    amazonS3Client.getObject(bucket, key).getObjectContent
  }
  
  override val fullpath = {
    "s3n://" + sys.env("AWS_ACCESS_KEY_ID") + ":" + sys.env("AWS_SECRET_ACCESS_KEY") + "@" + bucket + path
  }

}

class Hdfs(path: String, uri: String, username: String) extends CloudFileSystem(path, uri) {

  System.setProperty("HADOOP_USER_NAME", username)
  val conf = new Configuration()
  conf.set("fs.defaultFS", uri)
  val fs = FileSystem.get(conf)

  override def write(name: Int, data: Array[Byte]): Unit = {
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
