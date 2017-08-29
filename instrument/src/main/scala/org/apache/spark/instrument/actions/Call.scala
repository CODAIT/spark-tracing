package org.apache.spark.instrument.actions

import javassist._
import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class FunctionCall(name: String, duration: Long, args: Seq[Any], ret: Any)

object Call {
  def log(start: Long, name: String, args: Array[Any], ret: Any): Unit = {
    TraceWriter.log(start, FunctionCall(name, System.currentTimeMillis - start, args.toSeq, ret))
  }
}

class Call(cls: String, name: String) extends MethodInstrumentation {
  override def matches(method: CtMethod): Boolean = {
    method.getDeclaringClass.getName == cls && method.getName == name
  }
  override def apply(method: CtMethod): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq("start", str(method.getLongName), "$args", "ret"))
    wrap(method, "long start = System.currentTimeMillis();", report)
  }
}
