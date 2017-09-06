package org.apache.spark.instrument.tracers

/*import javassist._

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}
import org.apache.spark.scheduler.SparkListenerEvent

object Listener {
  def log(event: SparkListenerEvent): Unit = {
    TraceWriter.log(System.currentTimeMillis, event)
  }
}

class Listener extends MethodInstrumentation {
  override def matches(method: CtBehavior): Boolean = {
    check(method, "org.apache.spark.scheduler.SparkListener")
  }
  override def apply(method: CtBehavior): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "log", Seq("$1")))
  }
}*/
