package org.apache.spark.instrument

import org.scalatest._

class UtilTest extends FlatSpec with Matchers {
  private val in = Spark.sc.parallelize(SampleEvents.time).map(EventTree(_))

  "tokenize" should "split strings into matching and non-matching portions" in {
    def tok(s: String, re: String) = Util.tokenize(s, re)
    tok("a b", "\\s") shouldBe Seq("a", " ", "b")
    tok("xaxxbx", "x") shouldBe Seq("", "x", "a", "x", "", "x", "b", "x", "")
    tok("abc", "[abc]") shouldBe Seq("", "a", "", "b", "", "c", "")
    tok(" test3.14steins;gate x", "[^a-z]+") shouldBe Seq("", " ", "test", "3.14", "steins", ";", "gate", " ", "x")
  }

  "timeDelta" should "compute time differences" in {
    StatUtils.timeDelta(in.filter(!_(3)(0).isAny(Seq("SpanStart", "SpanEnd"))), _(4).is("start"), _(4).is("end"), _(3).get)
      .shouldBe(Map("a" -> 9, "b" -> 5, "c" -> -2))
  }

  "spanLength" should "compute span lengths" in {
    StatUtils.spanLength(in.filter(_(3)(0).isAny(Seq("SpanStart", "SpanEnd"))), "get")
      .shouldBe(Map("a" -> 2, "b" -> 3, "c" -> -1))
  }
}
