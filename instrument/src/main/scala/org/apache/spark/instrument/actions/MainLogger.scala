package org.apache.spark.instrument.actions

import java.lang.management.ManagementFactory
import javassist._

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class JVMStart(duration: Long)
case object MainEnd

object MainLogger {
  var alreadyLogged = false
  def logStart(): Unit = {
    if (!alreadyLogged) {
      alreadyLogged = true
      val start = ManagementFactory.getRuntimeMXBean.getStartTime
      TraceWriter.log(start, JVMStart(System.currentTimeMillis - start))
    }
  }
}

class MainLogger extends MethodInstrumentation {
  override def matches(method: CtMethod): Boolean = {
    check(method, None, Some("main"))
  }
  override def apply(method: CtMethod): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "logStart", Seq()))
    //method.insertAfter(functionCall(this.getClass.getCanonicalName, "logEnd", Seq()))
  }
}
