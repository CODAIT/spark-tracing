package org.apache.spark.instrument.tracers

import java.util.UUID
import javassist._
import org.apache.spark.instrument._

object Call {
  def log(start: Long, name: String, args: Array[Any], ret: Any): Unit = {
    val end = System.currentTimeMillis
    val id: UUID = UUID.randomUUID
    val call = FunctionCall(name, args.toSeq, ret)
    TraceWriter.log(start, ProcessStart(id, call))
    TraceWriter.log(end, ProcessEnd(id, call))
  }
}

class Call(cls: String, name: String) extends MethodInstrumentation {
  override def matches(method: CtBehavior): Boolean = check(method, cls, name)

  override def apply(method: CtBehavior): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq("start", str(method.getLongName), "$args", "ret"))
    wrap(method, "long start = System.currentTimeMillis();", report)
  }
}
