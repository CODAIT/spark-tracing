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