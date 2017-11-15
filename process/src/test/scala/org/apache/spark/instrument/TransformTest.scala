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

import org.json4s.native.JsonMethods
import org.scalatest._

class TransformTest extends FlatSpec with Matchers {
  "FormatSpec" should "format event trees" in {
    Set("test", "$1 test $2.2.1$r.4", "$$x$").foreach(spec => new FormatSpec(spec).toString shouldBe spec)

    def check(spec: String, tree: String): String = new FormatSpec(spec).format(EventTree(tree, true))
    check("asd", "(x,a(b),c)") shouldBe "asd"
    check("x$1x$1.0x", "(x,x(a(b,c)),d)") shouldBe "xa(b, c)xax"
    check("$1.1$1.1$1.2$0", "(x,y(a(b,c)),d)") shouldBe "bbcy"
    check("$r.3", "(x,a,b(c,d,e(f)))") shouldBe "e(f)"
    check("$1", "(a,b,c)") shouldBe ""
    check("$1.1", "(a,b(x,y),c)") shouldBe ""
    check("$r", "(a,b)") shouldBe ""
  }

  "EventFilterSpec" should "filter out non-matching events" in {
    def check(spec: Seq[String], tree: String): Boolean =
      EventFilterSpec(spec.map(Cond(_)), true).matches(EventTree(tree, true))

    val t1 = Seq("1 = a")
    check(t1, "(a)") shouldBe true
    check(t1, "(a,b)") shouldBe true
    check(t1, "(a())") shouldBe true
    check(t1, "()") shouldBe false
    check(t1, "(b,a)") shouldBe false
    check(t1, "((a))") shouldBe false
    check(t1, "") shouldBe false

    val t2 = Seq("1 in a,b")
    check(t2, "(a)") shouldBe true
    check(t2, "(a,a)") shouldBe true
    check(t2, "(b,x(bb))") shouldBe true
    check(t2, "(x)") shouldBe false
    check(t2, "(b(a))") shouldBe false

    val t3 = Seq("1 in a,b", "2 != y")
    check(t3, "(a,a,a)") shouldBe true
    check(t3, "(b,a)") shouldBe true
    check(t3, "(b,y(x))") shouldBe true
    check(t3, "(a,y)") shouldBe false
    check(t3, "(x,y(x))") shouldBe false

    val t4 = Seq("2.1 exists")
    check(t4, "(a,b(c))") shouldBe true
    check(t4, "(a,(b),a)") shouldBe true
    check(t4, "(a(b),c(d,e),f)") shouldBe true
    check(t4, "(a)") shouldBe false
    check(t4, "(a,b,c(d))") shouldBe false
    check(t4, "(a,b(),c)") shouldBe false
    check(t4, "()") shouldBe false

    val t5 = Seq("3.1 exists", "3.1 != x", "2 = a")
    check(t5, "(a,a,a(a))") shouldBe true
    check(t5, "(b(c),a,(y))") shouldBe true
    check(t5, "(a,a,a(a()))") shouldBe true
    check(t5, "(a,a,a(a(a)))") shouldBe true
    check(t5, "(a,a,a)") shouldBe false
    check(t5, "(a,a,a())") shouldBe false
    check(t5, "(a,a,a(x))") shouldBe false
    check(t5, "(a,b,a(a))") shouldBe false
    check(t5, "(a,a(a),a(a))") shouldBe false
    check(t5, "(a,a(a),a)") shouldBe false
    check(t5, "(a,,b(c))") shouldBe false

    val t6 = Seq("3 = a(b, c)")
    check(t6, "(d,e,a(b,c),d)") shouldBe true
    check(t6, "(x,x,a(b(),c),x(x))") shouldBe true
    check(t6, "(a(b,c))") shouldBe false
    check(t6, "(a,a,a(b))") shouldBe false
    check(t6, "(a,a,a(b,c(d)),e)") shouldBe false
  }

  "CaseParseSpec" should "parse case class strings in-place" in {
    val ev = EventTree(JsonMethods.parse(SampleEvents.json("case")))
    ev(1).get shouldBe "test"
    ev(2).get shouldBe SampleEvents.unparsed1
    ev(2)(0).get shouldBe SampleEvents.unparsed1
    ev(2)(1).isDefined shouldBe false
    ev(3)(2).get shouldBe SampleEvents.unparsed2
    ev(3)(3).get shouldBe "y"
    ev(3)(4).isDefined shouldBe false

    val parse1 = CaseParseSpec(Seq(Cond("1 = test"), Cond("3.0 = Test")), Seq(Seq(2), Seq(3, 2))).apply(ev)
    parse1(2)(0).get shouldBe "name"
    parse1(2)(1).isDefined shouldBe true
    parse1(2)(1).get shouldBe "arg1"
    parse1(2)(2).get shouldBe "arg2(arg2.1)"
    parse1(2)(2)(1).isDefined shouldBe false
    parse1(3)(2)(1).isDefined shouldBe true
    parse1(3)(2)(2).get shouldBe "arg3.2"
    parse1(3)(2)(3).isDefined shouldBe false

    val partial = CaseParseSpec(Seq(Cond("1 = test"), Cond("3.0 = Test")), Seq(Seq(3, 2))).apply(ev)
    partial(2).get shouldBe SampleEvents.unparsed1
    partial(2)(1).isDefined shouldBe false
    partial(3)(2)(1).isDefined shouldBe true
    partial(3)(2)(0).get shouldBe "arg3"
    partial(3)(2)(2).get shouldBe "arg3.2"

    val misparse1 = CaseParseSpec(Seq(Cond("1 = wrong")), Seq(Seq(2), Seq(3, 2))).apply(ev)
    misparse1(2).get shouldBe SampleEvents.unparsed1
    misparse1(2)(0).get shouldBe SampleEvents.unparsed1
    misparse1(2)(1).isDefined shouldBe false
    misparse1(3)(2).get shouldBe SampleEvents.unparsed2

    val misparse2 = CaseParseSpec(Seq(Cond("5 = out of bounds")), Seq(Seq(2))).apply(ev)
    misparse2(2)(0).get shouldBe SampleEvents.unparsed1
    misparse2(2)(1).isDefined shouldBe false
    misparse2(3)(2).get shouldBe SampleEvents.unparsed2

  }
}
