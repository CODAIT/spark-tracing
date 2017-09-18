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
