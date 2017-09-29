/* Copyright 2017 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.instrument.tracers

import java.util.UUID
import javassist._
import org.apache.spark.instrument._

case class SpanStart(id: UUID, process: Any) extends TraceEvent
case class SpanEnd(id: UUID) extends TraceEvent

object Span {
  def log(start: Long, name: String, args: Array[Any], ret: Any): Unit = TraceWriter.runAsOverhead {
    val end = System.currentTimeMillis
    val id: UUID = UUID.randomUUID
    val call = Fn(name, args.toSeq.map(Tracer.arrayWrap), Tracer.arrayWrap(ret))
    TraceWriter.log(start, SpanStart(id, call))
    TraceWriter.log(end, SpanEnd(id))
  }
}

class Span(cls: String, name: String) extends Tracer {
  override def matches(method: CtBehavior): Boolean = check(method, cls, name)

  override def apply(method: CtBehavior): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq("start", str(method.getLongName), "$args", "ret"))
    wrap(method, "long start = System.currentTimeMillis();", report)
  }
}
