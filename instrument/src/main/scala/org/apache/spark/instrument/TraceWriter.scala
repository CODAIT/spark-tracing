package org.apache.spark.instrument

import java.io.{File, FileWriter}
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.instrument.tracers.MainEnd

case class InstrumentOverhead(time: Long) extends TraceEvent

case class LogLine(time: Long, event: TraceEvent) {
  def format(traceid: String): String = {
    Seq(traceid, time, event.format()).mkString("\t")
  }
}

object TraceWriter {
  private val writeq = new LinkedBlockingQueue[LogLine]()

  private val overhead = new AtomicLong(0L)

  private val writer = new Thread { override def run(): Unit = {
    val id: String = UUID.randomUUID.toString

    val out = {
      val outdir = new File(Config.get[String]("output").getOrElse(throw new RuntimeException("Missing configuration props.output")))
      val cannotCreate = new RuntimeException("Cannot create trace output directory " + outdir)
      if (outdir.exists && !outdir.isDirectory) throw cannotCreate
      if (!outdir.exists && !outdir.mkdirs()) throw cannotCreate
      new FileWriter(s"${outdir.toString}/$id.tsv")
    }

    out.write("id\ttime\ttype\n")

    def writeOne(): Unit = {
      val line = writeq.take
      out.write(line.format(id) + "\n")
      if (line.event == MainEnd) {
        if (Config.trackOverhead)
          out.write(LogLine(System.currentTimeMillis, InstrumentOverhead(overhead.get)).format(id))
        out.close()
      }
      else writeOne()
    }

    writeOne() // Start the write loop
  }}

  def log(time: Long, event: TraceEvent): Unit = writeq.put(LogLine(time, event))

  def runAsOverhead[T](f: => T): T = {
    if (Config.trackOverhead) {
      val start = System.currentTimeMillis
      val ret = f
      overhead.addAndGet(System.currentTimeMillis - start)
      ret
    }
    else f
  }

  writer.start()
}
