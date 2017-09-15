package org.apache.spark.instrument

import java.io.{File, FileWriter}
import org.apache.spark.rdd.RDD

trait OutputBlockGenerator {
  def apply(events: RDD[EventTree], services: ServiceMap): OutputBlock
}

trait OutputBlock {
  def name: String
  def columns: Seq[String]
  def data: Iterable[Seq[Any]]
}

class Output(file: File) {
  private val out = new FileWriter(file)

  private def writeln() = out.write("\n")
  private def writeln(line: String) = out.write(line + "\n")
  private def writeln(line: Seq[Any]) = out.write(line.mkString("\t") + "\n")

  def addBlock(block: OutputBlock): Unit = {
    writeln(block.name)
    writeln(block.columns)
    block.data.foreach(row => writeln(row))
    writeln()
  }

  def close(): Unit = out.close()
}
