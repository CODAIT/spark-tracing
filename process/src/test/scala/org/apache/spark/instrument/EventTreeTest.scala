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

import org.scalatest._
import org.json4s.native.JsonMethods

class EventTreeTest extends FlatSpec with Matchers {
  "EventTree" should "parse JSON input" in {
    val ev = EventTree(JsonMethods.parse(SampleEvents.json("good")))
    Seq[EventTree => Assertion](
      _.isDefined shouldBe true,
      _(0).get shouldBe "Tuple3",
      _(1).get shouldBe "x",
      _(3).isDefined shouldBe true,
      _(4).isDefined shouldBe false,
      _(3)(0).getOption shouldBe Some("RPC"),
      _(3)(1).is("a") shouldBe true,
      _(3)(3).get shouldBe """I'm a "request"""",
      _(3)(4).isDefined shouldBe false,
      an [IndexOutOfBoundsException] should be thrownBy _(3)(4).get
    ).foreach(_(ev))
  }

  it should "reject JSON without the proper structure" in {
    a [RuntimeException] should be thrownBy EventTree(JsonMethods.parse(SampleEvents.json("bad")))
  }

  it should "parse well-formed tree strings" in {
    Map[String, Seq[EventTree => Assertion]](
      "simple" -> Seq(
        _(0).isDefined shouldBe true,
        _(0).is("name") shouldBe true,
        _(0).get shouldBe "name",
        _(0)(0)(0).is("name") shouldBe true,
        _(0)(0)(0).get shouldBe "name",
        _(Seq(1, 0, 0)).get shouldBe "arg1",
        _(2)(0).getOption shouldBe Some("arg2"),
        _(3).getOption shouldBe None,
        _(3).is("") shouldBe false,
        _.isDefined shouldBe true,
        _(2).isDefined shouldBe true,
        _(4).isDefined shouldBe false,
        _(0)(1).isDefined shouldBe false,
        _.toString shouldBe "name(arg1, arg2)"
      ),
      "typical" -> Seq(
        an [IndexOutOfBoundsException] should be thrownBy _(0).get,
        _(Seq(3, 2, 1, 4, 0, 0)).toString shouldBe "int",
        _(3)(2)(0).is("Fn") shouldBe true,
        _(3)(2).toString.length shouldBe 390
      ),
      "shallow" -> Seq(
        _(0).is("q") shouldBe true,
        _(0).isAny(Seq("q", "w", "2")) shouldBe true,
        _(1).isDefined shouldBe false
      ),
      "deep" -> Seq(
        _(Seq(1, 1, 1, 1, 2, 0)).get shouldBe "g",
        _(1)(1)(2)(1)(1)(2).get shouldBe "u",
        _(5)(12).toString shouldBe "O(P, Q(R), S)"
      ),
      "parens" -> Seq(
        _(1)(3)(1).isNull shouldBe true,
        an [IndexOutOfBoundsException] should be thrownBy _(1)(4)(1)(0).get,
        _(Seq(1, 4, 1, 1, 1, 1, 2)).toString shouldBe "",
        _(Seq(1, 4, 2, 1)).isDefined shouldBe false,
        _.isNull shouldBe true,
        _.toString shouldBe ""
      ),
      "special" -> Seq(
        _(1).get shouldBe "}}]",
        _(3)(3)(1).isDefined shouldBe true,
        _(3)(3).toString shouldBe """<""(")""",
        _(4)(4).get shouldBe ";;%;["
      ),
      "whitespace" -> Seq(
        _(0).get shouldBe "a",
        _(1)(2)(0).get shouldBe "d",
        _(1)(2).toString shouldBe "d(e, f, g(h, i), j)"
      ),
      "empty" -> Seq(
        an [IndexOutOfBoundsException] should be thrownBy _(1).get,
        _(3)(0).get shouldBe "a",
        _(6)(1).isDefined shouldBe true,
        _(6)(2).isDefined shouldBe false,
        _(6).toString shouldBe "b(c, )"
      ),
      "nested" -> Seq(
        _(Seq(1, 1, 1, 1, 1, 1)).get shouldBe "a",
        _(Seq(1, 1, 1, 1)).toString shouldBe "((a))",
        _(2).isDefined shouldBe false
      ),
      "nothing" -> Seq(
        _.isDefined shouldBe false,
        an [IndexOutOfBoundsException] should be thrownBy _.get,
        _.toString shouldBe "",
        _(1).isDefined shouldBe false
      )
    ).foreach { case (name, tests) =>
      val ev = EventTree(SampleEvents.good(name), true)
      tests.foreach(_(ev))
    }
  }

  it should "reject ill-formed tree strings" in {
    SampleEvents.bad.values.foreach(str => an [Exception] should be thrownBy EventTree(str, true))
  }

  it should "update arbitrary elements" in {
    EventTree("a(b,c(d,e),f)", true).update(Seq(2), "g").toString shouldBe "a(b, g, f)"
    EventTree("a(b(c(d(e, f))))", true).update(Seq(1), _(1)(1)).toString shouldBe "a(d(e, f))"
    EventTree("a").update(Seq(), "b").toString shouldBe "b"
  }
}
