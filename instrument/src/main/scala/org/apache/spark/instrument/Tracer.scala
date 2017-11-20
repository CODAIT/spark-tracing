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

package org.apache.spark.instrument

import javassist._

trait TraceEvent

case class Fn(name: String, args: Seq[Any], ret: Any) extends TraceEvent

object Tracer {
  def box(x: Boolean) = new java.lang.Boolean(x)
  def box(x: Byte) = new java.lang.Byte(x)
  def box(x: Char) = new java.lang.Character(x)
  def box(x: Float) = new java.lang.Float(x)
  def box(x: Int) = new java.lang.Integer(x)
  def box(x: Long) = new java.lang.Long(x)
  def box(x: Short) = new java.lang.Short(x)
  def box(x: Double) = new java.lang.Double(x)
  def box(x: AnyRef): AnyRef = x
}

abstract class Tracer {
  final val prefix = "sparkTracingInstr_219d8d0c096b4facbf94d7fb89fb2f77_" // Try to be unique

  protected def str(s: String): String = "\"" + s + "\""

  protected def functionCall(cls: String, method: String, args: Seq[String]): String = {
    " " + cls + "." + method + args.mkString("(", ", ", ")") + "; "
  }

  protected def check(method: CtBehavior, cls: String, name: String = "*"): Boolean = {
    (cls == "*" || cls == method.getDeclaringClass.getName) && (name == "*" || name == method.getName)
  }
  def matches(method: CtBehavior): Boolean

  def apply(method: CtBehavior): Unit
}
