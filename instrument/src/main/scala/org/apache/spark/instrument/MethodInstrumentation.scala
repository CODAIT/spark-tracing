package org.apache.spark.instrument

import java.util.UUID
import javassist._

trait MethodType
case class Method(name: String) extends MethodType
case object Constructor extends MethodType

case class ProcessStart(id: UUID, process: Any)
case class ProcessEnd(id: UUID, process: Any)

abstract class MethodInstrumentation() {
  final val prefix = "StcSparkInstr_"

  protected def str(s: String): String = "\"" + s + "\""

  protected def functionCall(cls: String, method: String, args: Seq[String]): String = {
    " " + cls + "." + method + args.mkString("(", ", ", ")") + "; "
  }

  protected def wrap(method: CtMethod, before: String, after: String): CtMethod = {
    val cls = method.getDeclaringClass
    val original = CtNewMethod.copy(method, prefix + method.getName, cls, null)
    cls.addMethod(original)
    val ret = method.getReturnType.getName
    val body = "{ " + before +
      (if (ret != "void") ret + " ret = " else "Object ret = null; ") +
      original.getName + "($$);" + after + (if (ret != "void") "return ret;" else "") + " }"
    method.setBody(body)
    method
  }

  protected def wrap(method: CtConstructor, before: String, after: String): CtConstructor = {
    ???
  }

  protected def check(method: CtBehavior, cls: Option[String], name: Option[String]): Boolean = {
    !method.isEmpty && (cls.isEmpty || cls.get == method.getDeclaringClass.getName) && (name.isEmpty || name.get == method.getName)
  }
  protected def check(method: CtBehavior, cls: String, name: String): Boolean = check(method, Some(cls), Some(name))
  protected def check(method: CtBehavior, cls: String): Boolean = check(method, Some(cls), None)

  def matches(method: CtMethod): Boolean

  def apply(method: CtMethod): Unit
}
