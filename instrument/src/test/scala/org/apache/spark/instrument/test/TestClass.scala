package org.apache.spark.instrument.test

import org.apache.spark.instrument.scaffold._

class BasicTest extends TestClass {
  override def foo(x: Int): Int = x + 10
}

class MemberTest(a: Int) extends TestClass {
  override def foo(x: Int): Int = x * a
}

class ConstructorTest(a: Int) extends TestClass {
  override def foo(x: Int): Int = a
}

class TraitConstructTest(a: ArgTrait) extends TestClass {
  override def foo(x: Int): Int = a.bar + TraitConstructTest.increment
}

object TraitConstructTest {
  val increment = 1
}