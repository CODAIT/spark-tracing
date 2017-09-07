package org.apache.spark.instrument.tracers

import java.util.UUID
import javassist._
import org.apache.spark.instrument._

case class SpanStart(id: UUID, process: Any) extends TraceEvent
case class SpanEnd(id: UUID, process: Any) extends TraceEvent

object Span {
  def log(start: Long, name: String, args: Array[Any], ret: Any): Unit = {
    val end = System.currentTimeMillis
    val id: UUID = UUID.randomUUID
    val call = Fn(name, args.toSeq, ret)
    TraceWriter.log(start, SpanStart(id, call))
    TraceWriter.log(end, SpanEnd(id, call))
  }
}

class Span(cls: String, name: String) extends Tracer {
  override def matches(method: CtBehavior): Boolean = check(method, cls, name)

  override def apply(method: CtBehavior): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq("start", str(method.getLongName), "$args", "ret"))
    wrap(method, "long start = System.currentTimeMillis();", report)
  }
}
