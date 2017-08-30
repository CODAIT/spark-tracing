package org.apache.spark.instrument.tracers

import java.util.UUID
import javassist._
import org.apache.spark.instrument._

case class FunctionCall(name: String, args: Seq[Any], ret: Any)

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
  override def matches(method: CtMethod): Boolean = {
    check(method, cls, name)
  }
  override def apply(method: CtMethod): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq("start", str(method.getLongName), "$args", "ret"))
    wrap(method, "long start = System.currentTimeMillis();", report)
  }
}
