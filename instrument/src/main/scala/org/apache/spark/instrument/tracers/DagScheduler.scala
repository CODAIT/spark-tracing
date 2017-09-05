package org.apache.spark.instrument.tracers

import javassist._
import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class DagSchedulerEvent(event: Any)

object DagScheduler {
  def log(event: Any): Unit =
    TraceWriter.log(System.currentTimeMillis, DagSchedulerEvent(event))
}

class DagScheduler extends MethodInstrumentation {
  override def matches(method: CtBehavior): Boolean =
    check(method, "org.apache.spark.scheduler.DAGSchedulerEventProcessLoop", "doOnReceive")
  override def apply(method: CtBehavior): Unit =
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "log", Seq("$1")))
}
