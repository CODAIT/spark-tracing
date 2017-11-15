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

import org.apache.spark.instrument.scaffold._
import org.apache.spark.instrument.test._
import org.scalatest._

class InstrumentTest extends FlatSpec with Matchers {
  val loader = new TestLoader(Thread.currentThread().getContextClassLoader)
  val classes: Map[String, Class[_]] = Set(
    "BasicTest",
    "MemberTest",
    "ConstructorTest",
    "TraitConstructTest"
  ).map(x => x -> loader.loadClass("org.apache.spark.instrument.test." + x)).toMap

  "ClassInstrumenter" should "instrument classes" in {
    val uninst: TestClass = new BasicTest()
    val inst: TestClass = classes("BasicTest").newInstance().asInstanceOf[TestClass]
    uninst.foo(12) shouldBe 22
    inst.foo(12) shouldBe 32
  }

  it should "work with instance variables" in {
    val uninst: TestClass = new MemberTest(2)
    val inst = classes("MemberTest").getDeclaredConstructors.head.newInstance(new Integer(2)).asInstanceOf[TestClass]
    uninst.foo(3) shouldBe 6
    inst.foo(3) shouldBe 12
  }

  it should "work with constructors" in {
    val uninst: TestClass = new ConstructorTest(6)
    val inst = classes("ConstructorTest").getDeclaredConstructors.head.newInstance(new Integer(6)).asInstanceOf[TestClass]
    uninst.foo(0) shouldBe 6
    inst.foo(0) shouldBe -6
  }

  it should "handle more complicated constructors" in {
    val arg: ArgTrait = ConstructorArg(98)
    val uninst: TestClass = new TraitConstructTest(arg)
    val inst = classes("TraitConstructTest").getDeclaredConstructors.head.newInstance(arg).asInstanceOf[TestClass]
    uninst.foo(0) shouldBe 99
    inst.foo(0) shouldBe 100
  }
}
