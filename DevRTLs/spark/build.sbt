organization := "org.llvm.openmp"

name := "omptarget-spark"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.6"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" %   "1.10.57"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)
