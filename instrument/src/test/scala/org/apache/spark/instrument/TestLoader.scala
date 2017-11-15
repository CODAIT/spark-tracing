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
