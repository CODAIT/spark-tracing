package org.apache.spark.instrument

import javassist._

trait MethodType
case class Method(name: String) extends MethodType
case object Constructor extends MethodType

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

  def matches(method: CtMethod): Boolean

  def apply(method: CtMethod): Unit
}
