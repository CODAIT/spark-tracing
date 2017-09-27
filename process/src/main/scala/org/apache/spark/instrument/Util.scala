package org.apache.spark.instrument

object Util {
  /*import scala.collection.mutable.ListBuffer
  private def psplit(s: String): Seq[String] = {
    var lvl = 0
    val cur = new StringBuilder
    val ret = new ListBuffer[String]
    s.foreach { c =>
      if (c == '(') lvl += 1
      else if ( c == ')') lvl -= 1
      if (c == ',' && lvl == 0) {
        ret += cur.toString.trim
        cur.clear
      }
      else cur += c
    }
    if (cur.nonEmpty) ret += cur.toString.trim
    ret
  }*/

  def substrIdx(s: String, start: Int, c: Char): Int = s.zipWithIndex.drop(start + 1).find(_._1 == c).map(_._2).getOrElse(-1)

  def pskip(s: String, start: Int): Int = {
    val open = substrIdx(s, start, '(')
    val close = substrIdx(s, start, ')')
    if (close < 0) throw new RuntimeException("Mismatched parentheses")
    if (open < 0 || open > close) close
    else pskip(s, pskip(s, open))
  }

  def psplit(s: String, sep: Char = ','): List[String] = {
    val firstSep = s.indexOf(sep)
    val firstParen = s.indexOf('(')
    if (firstSep < 0) List(s)
    else if (firstParen < 0) {
      if (s.indexOf(')') >= 0) throw new RuntimeException("Mismatched parentheses")
      s.split(sep).toList
    }
    else if (firstSep < firstParen) s.substring(0, firstSep) :: psplit(s.substring(firstSep + 1), sep)
    else {
      val nextStart = pskip(s, firstParen) + 1
      val next = psplit(s.substring(nextStart), sep)
      s.substring(0, nextStart) + next.head :: next.tail
    }
  }

  def interleave[T](a: Seq[T], b: Seq[T]): Seq[T] = {
    if (a.isEmpty) b
    else if (b.isEmpty) a
    else a.head +: b.head +: interleave(a.tail, b.tail)
  }

  def tokenize(str: String, re: String): Seq[String] = {
    val matches = re.r.findAllIn(str).toSeq
    val nonmatches = str.split(re)
    assert(matches.size == nonmatches.size - 1)
    interleave(nonmatches, matches)
  }
}
