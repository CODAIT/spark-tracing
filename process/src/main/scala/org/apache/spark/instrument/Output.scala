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

package org.apache.spark.instrument

import java.io.{File, FileWriter}

trait ColType
case object Num extends ColType { override def toString: String = "num" }
case object Time extends ColType { override def toString: String = "time" }
case object Str extends ColType { override def toString: String = "string" }

trait OutputBlock extends Serializable {
  def name: String
  def columns: Seq[(String, ColType)]
  def data: Iterable[Seq[Any]]
}

class Output(file: File) {
  private val out = new FileWriter(file)

  private def writeln() = out.write("\n")
  private def writeln(line: String) = out.write(line + "\n")
  private def writeln(line: Seq[Any]) = out.write(line.mkString("\t") + "\n")

  def addBlock(block: OutputBlock): Unit = {
    writeln(block.name)
    writeln(block.columns.map(_._1))
    writeln(block.columns.map(_._2))
    block.data.foreach(row => writeln(row))
    writeln("%")
  }

  def close(): Unit = out.close()
}
