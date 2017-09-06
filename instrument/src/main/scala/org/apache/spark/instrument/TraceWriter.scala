package org.apache.spark.instrument

import java.io.{File, FileWriter}
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import org.apache.spark.instrument.tracers.MainEnd

case class LogLine(time: Long, event: TraceEvent) {
  def format(traceid: String): String = {
    Seq(traceid, time, event.format).mkString("\t")
  }
}

object TraceWriter {
  private val writeq = new LinkedBlockingQueue[LogLine]()

  private val writer = new Thread { override def run(): Unit = {
    val id: String = UUID.randomUUID.toString

    val out = {
      val outdir = new File("/tmp/spark-trace")
      val cannotCreate = new RuntimeException("Cannot create trace output directory " + outdir)
      if (outdir.exists && !outdir.isDirectory) throw cannotCreate
      if (!outdir.exists && !outdir.mkdirs()) throw cannotCreate
      new FileWriter(s"${outdir.toString}/$id.tsv")
    }

    out.write("id\ttime\ttype\n")

    while (true) {
      val line = writeq.take
      out.write(line.format(id) + "\n")
      if (line.event == MainEnd) {
        out.close()
        return
      }
    }
  }}

  def log(time: Long, event: TraceEvent): Unit = writeq.put(LogLine(time, event))

  writer.start()

}
