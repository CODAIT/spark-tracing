package org.apache.spark.instrument

import org.scalatest._

class TransformTest extends FlatSpec with Matchers {
  "FormatSpec" should "format event trees" in {
    Set("test", "$1 test $2.2.1$r.4", "$$x$").foreach(spec => new FormatSpec(spec).toString shouldBe spec)

    def check(spec: String, tree: String): String = new FormatSpec(spec).format(EventTree(tree))
    check("asd", "(x,a(b),c)") shouldBe "asd"
    check("x$1x$1.0x", "(x,x(a(b,c)),d)") shouldBe "xa(b, c)xax"
    check("$1.1$1.1$1.2$0", "(x,y(a(b,c)),d)") shouldBe "bbcy"
    check("$r.3", "(x,a,b(c,d,e(f)))") shouldBe "e(f)"
    check("$1", "(a,b,c)") shouldBe ""
    check("$1.1", "(a,b(x,y),c)") shouldBe ""
    check("$r", "(a,b)") shouldBe ""
  }

  "EventFilterSpec" should "filter out non-matching events" in {
    def check(spec: Seq[String], tree: String): Boolean = new EventFilterSpec(spec, true).matches(EventTree(tree))

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
}
