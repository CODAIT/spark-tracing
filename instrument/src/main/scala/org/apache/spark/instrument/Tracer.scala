package org.apache.spark.instrument

import java.util.UUID
import javassist._

trait TraceEvent {
  def format(): String = toString.split("\n")(0)
}

case class Fn(name: String, args: Seq[Any], ret: Any) extends TraceEvent

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

  protected def check(method: CtBehavior, cls: Option[String], name: Option[String]): Boolean = {
    !method.isEmpty &&
      (cls.isEmpty || cls.get == "*" || cls.get == method.getDeclaringClass.getName) &&
      (name.isEmpty || name.get == "*" || name.get == method.getName)
  }
  protected def check(method: CtBehavior, cls: String, name: String): Boolean = check(method, Some(cls), Some(name))
  protected def check(method: CtBehavior, cls: String): Boolean = check(method, Some(cls), None)

  def matches(method: CtBehavior): Boolean

  def apply(method: CtBehavior): Unit
}
