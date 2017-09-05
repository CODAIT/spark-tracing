package org.apache.spark.instrument.tracers

import java.lang.management.ManagementFactory
import java.util.UUID
import javassist._
import org.apache.spark.instrument._

//case class JVMStart(duration: Long)
case object JVMStart
case object MainEnd

object MainLogger {
  var alreadyLogged = false
  def logStart(): Unit = {
    if (!alreadyLogged) {
      alreadyLogged = true
      val end = System.currentTimeMillis
      val id = UUID.randomUUID
      val start = ManagementFactory.getRuntimeMXBean.getStartTime
      //TraceWriter.log(start, JVMStart(System.currentTimeMillis - start))
      TraceWriter.log(start, ProcessStart(id, JVMStart))
      TraceWriter.log(end, ProcessEnd(id, JVMStart))
    }
  }
}

class MainLogger extends MethodInstrumentation {
  override def matches(method: CtBehavior): Boolean = {
    check(method, None, Some("main"))
  }
  override def apply(method: CtBehavior): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "logStart", Seq()))
    //method.insertAfter(functionCall(this.getClass.getCanonicalName, "logEnd", Seq()))
  }
}
