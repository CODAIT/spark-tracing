name := "instrument"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "javassist" % "javassist" % "3.12.0.GA",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0"
)

packageOptions in (Compile, packageBin) +=
  Package.ManifestAttributes("Premain-Class" -> "org.apache.spark.instrument.SparkAgent")
