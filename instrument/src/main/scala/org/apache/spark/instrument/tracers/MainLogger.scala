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

import java.lang.management.ManagementFactory
import java.util.UUID
import javassist._
import org.apache.spark.instrument._

case object JVMStart extends TraceEvent
case object MainEnd extends TraceEvent

object MainLogger {
  var alreadyLogged = false

  def logStart(): Unit = TraceWriter.runAsOverhead {
    if (!alreadyLogged) {
      alreadyLogged = true
      val end = System.currentTimeMillis
      val id = UUID.randomUUID
      val start = ManagementFactory.getRuntimeMXBean.getStartTime
      TraceWriter.log(start, SpanStart(id, JVMStart))
      TraceWriter.log(end, SpanEnd(id))
    }
  }

  def logEnd(): Unit = TraceWriter.runAsOverhead {
    TraceWriter.log(System.currentTimeMillis(), MainEnd)
  }

  Runtime.getRuntime.addShutdownHook(new Thread { override def run(): Unit = {
    logEnd()
  }})
}

class MainLogger extends Tracer {
  override def matches(method: CtBehavior): Boolean = {
    check(method, "*", "main") && !method.isEmpty
  }

  override def apply(method: CtBehavior): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "logStart", Seq()))
    method.insertAfter(functionCall(this.getClass.getCanonicalName, "logEnd", Seq()))
  }
}
