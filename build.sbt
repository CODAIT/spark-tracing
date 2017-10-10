/* Copyright 2017 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "spark-tracing"

lazy val instrument = (project in file("instrument")).settings(
  name := "instrument",
  version := "1.0",
  scalaVersion := "2.11.8",
  libraryDependencies ++= Seq(
    "javassist" % "javassist" % "3.12.0.GA" % "provided",
    "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
    "com.typesafe" % "config" % "1.3.1",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  ),
  packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Premain-Class" -> "org.apache.spark.instrument.SparkAgent"),
  javaOptions in Test ++= Seq(
    "-Djava.system.class.loader=org.apache.spark.instrument.TestLoader",
    s"-Dinstrument.config=${baseDirectory.value}/src/test/resources/test.conf"
  ),
  fork in Test := true
)

lazy val process = (project in file("process")).settings(
  name := "process",
  version := "1.0",
  scalaVersion := "2.11.8",
  libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
    "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
    "com.typesafe" % "config" % "1.3.1",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
)

