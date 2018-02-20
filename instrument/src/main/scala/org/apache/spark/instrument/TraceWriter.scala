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

import java.net.URI
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.instrument.tracers.MainEnd
import org.json4s._
import org.json4s.native.Serialization

class EventSerializer extends CustomSerializer[Any](implicit formats => ( { case _: JValue => Unit },
  {
    case x if x == null => JNull
    case x: Product => new JObject(List(
      "type" -> JString(x.productPrefix),
      "fields" -> JArray(x.productIterator.map(Extraction.decompose).toList)
    ))
    case x: TraversableOnce[_] => new JObject(List(
      "type" -> JString("TraversableOnce"),
      "fields" -> JArray(x.map(Extraction.decompose).toList)
    ))
    case x: Array[_] => new JObject(List(
      "type" -> JString("Array"),
      "fields" -> JArray(x.map(Extraction.decompose).toList)
    ))
    case x => JString(x.toString)
  }
))

case class InstrumentOverhead(time: Long) extends TraceEvent

case class LogLine(time: Long, event: TraceEvent) {
  def format(traceid: String): String = {
    implicit val formats = org.json4s.DefaultFormats + new EventSerializer
    Serialization.write((traceid, time, event))
  }
}

object TraceWriter {
  private val writeq = new LinkedBlockingQueue[LogLine]()

  private val overhead = new AtomicLong(0L)

  private val writer = new Thread {
    setDaemon(true)

    override def run(): Unit = {
      val id: String = UUID.randomUUID.toString

      val out = {
        val outdir = Config.get[String]("traceout").getOrElse(throw new RuntimeException("Missing configuration props.traceout"))
        val fs = FileSystem.get(new URI(outdir), new Configuration)
        fs.mkdirs(new Path(outdir))
        fs.create(new Path(s"$outdir/$id.trace"))
      }

      sys.addShutdownHook(out.close())

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
    }
  }

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
