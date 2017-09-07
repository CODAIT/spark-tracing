package org.apache.spark.instrument.tracers

import javassist._
import org.apache.spark.instrument._

object Event {
  def log(name: String, args: Array[Any]): Unit = {
    TraceWriter.log(System.currentTimeMillis(), Fn(name, args.toSeq, null))
  }
}

class Event(cls: String, name: String) extends Tracer {
  override def matches(method: CtBehavior): Boolean = check(method, cls, name)

  override def apply(method: CtBehavior): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq(str(method.getLongName), "$args"))
    method.insertBefore(report)
  }
}
