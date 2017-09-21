package org.apache.spark.instrument

import java.net.URI
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.instrument.tracers.MainEnd

case class InstrumentOverhead(time: Long) extends TraceEvent

case class LogLine(time: Long, event: TraceEvent) {
  def format(traceid: String): String = {
    Seq(traceid, time, event.format()).mkString("(", ",", ")")
  }
}

object TraceWriter {
  private val writeq = new LinkedBlockingQueue[LogLine]()

  private val overhead = new AtomicLong(0L)

  private val writer = new Thread { override def run(): Unit = {
    val id: String = UUID.randomUUID.toString

    val out = {
      val outdir = Config.get[String]("traceout").getOrElse(throw new RuntimeException("Missing configuration props.traceout"))
      val fs = FileSystem.get(new URI(outdir), new Configuration)
      fs.mkdirs(new Path(outdir))
      fs.create(new Path(s"$outdir/$id.trace"))
    }

    def writeOne(): Unit = {
      val line = writeq.take
      out.write((line.format(id) + "\n").getBytes)
      if (line.event == MainEnd) {
        if (Config.trackOverhead)
          out.write(LogLine(System.currentTimeMillis, InstrumentOverhead(overhead.get)).format(id).getBytes)
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
