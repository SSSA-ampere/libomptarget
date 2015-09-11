package org.llvm.openmp

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD

class AddressTableEntry {
  var address: Long = 0
  var mapflag: Byte = 0
  var path: String = ""
}

class AddressTable(sc: SparkContext, info: HdfsInfo) {
  sc.addFile(info.fullpath + "__address_table")
  val bytearray = Files.readAllBytes(Paths.get(SparkFiles.get("__address_table")))

  var addressTable: Array[AddressTableEntry] = new Array[AddressTableEntry](0)

  // Read 8 bytes, then read 1 byte, then read until a null byte
  // Keep doing this until the end
  val buffer = ByteBuffer.wrap(bytearray).order(java.nio.ByteOrder.LITTLE_ENDIAN)
  var currpath = new Array[Byte](0)
  var currchar: Byte = 0

  while (buffer.hasRemaining()) {
    val current = new AddressTableEntry()

    current.address = buffer.getLong()
    current.mapflag = buffer.get()

    currpath = new Array[Byte](0)
    currchar = buffer.get()
    while (currchar != 0) {
      currpath = currpath :+ currchar
      currchar = buffer.get()
    }

    current.path = new String(currpath)
    addressTable = addressTable :+ current
  }

  def genRDDs(): Array[RDD[Array[Byte]]] = {
    var arguments = new Array[RDD[Array[Byte]]](0)

    // Read input as a set of 32 bits raw data
    for (i <- 0 to (addressTable.length - 1)) {
      if ((addressTable(i).mapflag & 0x2) == 0x2) {
        var currarg = sc.binaryRecords(info.uri + addressTable(i).path, 4)
        arguments = arguments :+ currarg
      }
    }

    return arguments
  }

  def apply(i: Integer): AddressTableEntry = {
    return addressTable(i)
  }
}