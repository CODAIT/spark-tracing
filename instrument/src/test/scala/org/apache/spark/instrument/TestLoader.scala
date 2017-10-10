package org.apache.spark.instrument

import javassist._

class TestLoader(parent: ClassLoader) extends ClassLoader(parent) {
  val p = "org.apache.spark.instrument.test."
  protected override def loadClass(name: String, resolve: Boolean): Class[_] = {
    if (!name.startsWith(p)) super.loadClass(name, resolve)
    else {
      val subname = name.substring(p.length)
      val cls = ClassPool.getDefault.get(p + subname)
      subname match {
        case "BasicTest" => cls.getDeclaredMethod("foo").setBody("return $1 + 20;")
        case "MemberTest" => cls.getDeclaredMethod("foo").insertBefore("a += 2;")
        case "ConstructorTest" => cls.getDeclaredConstructors.head.insertBefore("$1 *= -1;")
        case "TraitConstructTest" => cls.getDeclaredConstructors.head
          .insertBefore("$1 = new org.apache.spark.instrument.scaffold.ConstructorArg(99);")
        case _ => super.loadClass(name, resolve)
      }
      cls.toClass
    }
  }
}
