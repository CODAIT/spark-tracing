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

import java.util.UUID
import javassist._

trait TraceEvent {
  def format(): String = toString.replaceAll("[\\n\\t]", "; ")
}

case class Fn(name: String, args: Seq[Any], ret: Any) extends TraceEvent

object Tracer {
  def arrayWrap(x: Any): Any = {
    if (x.isInstanceOf[Array[_]]) x.asInstanceOf[Array[Any]].toSeq else x
  }
}

abstract class Tracer() {
  final val prefix = "sparkTracingInstr_"

  protected def str(s: String): String = "\"" + s + "\""

  protected def functionCall(cls: String, method: String, args: Seq[String]): String = {
    " " + cls + "." + method + args.mkString("(", ", ", ")") + "; "
  }

  protected def wrap(method: CtMethod, before: String, after: String): CtMethod = {
    val cls = method.getDeclaringClass
    val original = CtNewMethod.copy(method, prefix + method.getName, cls, null)
    cls.addMethod(original)
    val delegate = original.getName + "($$)"
    val body = method.getReturnType match {
      case x if x == CtClass.voidType => s"{ $before; Object ret = null; $delegate; $after; }"
      case rettype => s"{ $before; ${rettype.getName} ret = $delegate; $after; return ret; }"
    }
    method.setBody(body)
    method
  }

  protected def wrap(method: CtConstructor, before: String, after: String): CtConstructor = {
    val cls = method.getDeclaringClass
    val name = prefix + UUID.randomUUID.toString.replace("-", "")
    cls.addMethod(method.toMethod(name, cls))
    val body = "{ " + before + name + "($$); Object ret = this; " + after + " return ret; }"
    method.setBody(body)
    method
  }

  protected def wrap(method: CtBehavior, before: String, after: String): CtBehavior = {
    method match {
      case m: CtMethod => wrap(m, before, after)
      case m: CtConstructor => wrap(m, before, after)
      case _ => throw new RuntimeException("Behavior was not method or constructor")
    }
  }

  protected def check(method: CtBehavior, cls: String, name: String = "*"): Boolean = {
    (cls == "*" || cls == method.getDeclaringClass.getName) && (name == "*" || name == method.getName)
  }
  def matches(method: CtBehavior): Boolean

  def apply(method: CtBehavior): Unit
}
