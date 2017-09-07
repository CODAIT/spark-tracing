package org.apache.spark.instrument.tracers

import java.lang.management.ManagementFactory
import java.util.UUID
import javassist._
import org.apache.spark.instrument._

case object JVMStart extends TraceEvent
case object MainEnd extends TraceEvent

object MainLogger {
  var alreadyLogged = false

  def logStart(): Unit = {
    if (!alreadyLogged) {
      alreadyLogged = true
      val end = System.currentTimeMillis
      val id = UUID.randomUUID
      val start = ManagementFactory.getRuntimeMXBean.getStartTime
      //TraceWriter.log(start, JVMStart(System.currentTimeMillis - start))
      TraceWriter.log(start, SpanStart(id, JVMStart))
      TraceWriter.log(end, SpanEnd(id, JVMStart))
    }
  }

  def logEnd(): Unit = {
    TraceWriter.log(System.currentTimeMillis(), MainEnd)
  }

  Runtime.getRuntime.addShutdownHook(new Thread { override def run(): Unit = {
    logEnd()
  }})
}

class MainLogger extends Tracer {
  override def matches(method: CtBehavior): Boolean = {
    check(method, None, Some("main"))
  }

  override def apply(method: CtBehavior): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "logStart", Seq()))
    method.insertAfter(functionCall(this.getClass.getCanonicalName, "logEnd", Seq()))
  }
}
