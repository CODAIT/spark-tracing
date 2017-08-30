package org.apache.spark.instrument.tracers

import javassist.CtMethod

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}
import org.apache.spark.scheduler.{TaskSet, TaskSetManager}

trait TaskSetOp
case class SubmitTaskSet(id: String) extends TaskSetOp
case class FinishTaskSet(id: String) extends TaskSetOp

object TaskScheduler {
  def log(id: TaskSet): Unit = TraceWriter.log(System.currentTimeMillis, SubmitTaskSet(id.toString))
  def log(id: TaskSetManager): Unit = TraceWriter.log(System.currentTimeMillis, FinishTaskSet(id.taskSet.id))
}

class TaskScheduler extends MethodInstrumentation {
  override def matches(method: CtMethod): Boolean = {
    check(method, "org.apache.spark.scheduler.TaskSchedulerImpl") && Set("submitTasks", "taskSetFinished").contains(method.getName)
  }
  override def apply(method: CtMethod): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq("$1"))
    method.insertBefore(report)
  }
}
