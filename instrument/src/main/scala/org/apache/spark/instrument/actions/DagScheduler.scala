package org.apache.spark.instrument.actions

import javassist.CtMethod

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class DagSchedulerEvent(event: Any)

object DagScheduler {
  def log(event: Any): Unit =
    TraceWriter.log(System.currentTimeMillis, DagSchedulerEvent(event))
}

class DagScheduler extends MethodInstrumentation {
  override def matches(method: CtMethod): Boolean =
    check(method, "org.apache.spark.scheduler.DAGSchedulerEventProcessLoop", "doOnReceive")
  override def apply(method: CtMethod): Unit =
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "log", Seq("$1")))
}
