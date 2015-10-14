organization := "org.llvm.openmp"

name := "omptarget-spark"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
