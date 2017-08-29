package org.apache.spark.instrument.actions

import javassist._
import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class Exit(code: Int)

object ExitLogger {
  def log(code: Int): Unit = {
    TraceWriter.log(System.currentTimeMillis, Exit(code))
  }
}

class ExitLogger extends MethodInstrumentation {
  override def matches(method: CtMethod): Boolean = {
    method.getDeclaringClass.getName == "java.lang.Shutdown" && method.getName == "exit"
  }
  override def apply(method: CtMethod): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "log", Seq("$1")))
  }
}
