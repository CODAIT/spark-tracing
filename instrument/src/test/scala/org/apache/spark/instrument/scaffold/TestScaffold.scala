package org.apache.spark.instrument.scaffold

trait TestClass { def foo(x: Int): Int }

trait ArgTrait { def bar: Int }

case class ConstructorArg(x: Int) extends ArgTrait {
  override def bar: Int = x
}
