package org.apache.spark.instrument.tracers

import javassist._

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class ShuffleOp(cls: String, func: String)

object Shuffles {
  def log(cls: String, func: String): Unit = {
    TraceWriter.log(System.currentTimeMillis, ShuffleOp(cls, func))
  }
}

class Shuffles extends MethodInstrumentation {
  def isManager(method: CtBehavior): Boolean =
    check(method, "org.apache.spark.shuffle.sort.SortShuffleManager") && method.getName.endsWith("registerShuffle")
  def isMaster(method: CtBehavior): Boolean =
    check(method, "org.apache.spark.MapOutputTrackerMaster") && method.getName.endsWith("registerShuffle")
  override def matches(method: CtBehavior): Boolean = isManager(method) || isMaster(method)

  override def apply(method: CtBehavior): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq(method.getDeclaringClass.getName, method.getName))
  }
}
