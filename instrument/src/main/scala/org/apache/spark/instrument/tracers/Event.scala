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

import javassist._
import org.apache.spark.instrument._

object Event {
  def log(name: String, args: Array[Any]): Unit = TraceWriter.runAsOverhead {
    TraceWriter.log(System.currentTimeMillis(), Fn(name, args.toSeq.map(Tracer.arrayWrap), null))
  }
}

class Event(cls: String, name: String) extends Tracer {
  override def matches(method: CtBehavior): Boolean = check(method, cls, name)

  override def apply(method: CtBehavior): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq(str(method.getLongName), "$args"))
    method.insertBefore(report)
  }
}
