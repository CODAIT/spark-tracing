package org.apache.spark.instrument.tracers

import javassist._
import org.apache.spark.instrument._

case class ClientMethod(name: String)

object ClientLogger {
  def log(name: String): Unit = {
    TraceWriter.log(System.currentTimeMillis, ClientMethod(name))
  }
}

class ClientLogger extends MethodInstrumentation {
  override def matches(method: CtBehavior): Boolean = {
    check(method, "org.apache.spark.deploy.yarn.Client")
  }
  override def apply(method: CtBehavior): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "log", Seq(method.getName)))
  }
}
