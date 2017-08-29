package org.apache.spark.instrument

import java.io.{File, FileWriter}
import java.util.UUID

object TraceWriter {
  final val id: String = UUID.randomUUID.toString

  private val out = {
    val outdir = new File("/tmp/spark-trace")
    val cannotCreate = new RuntimeException("Cannot create trace output directory " + outdir)
    if (outdir.exists && !outdir.isDirectory) throw cannotCreate
    if (!outdir.exists && !outdir.mkdirs()) throw cannotCreate
    new FileWriter(s"${outdir.toString}/$id.tsv")
  }

  private def write(line: String): Unit = {
    out.write(line + "\n")
    out.flush()
  }
  // TODO add shutdown hook to close out
  // TODO synchronize write...maybe make async
  def log(time: Long, event: Any): Unit = {
    write(Seq(id, time, event.toString.split("\n")(0)).mkString("\t"))
  }

  out.write("id\ttime\ttype\n")
}
