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
    val partialNonmatches = str.split(re).toSeq
    // Tragically, Regex.split doesn't appear to split if the regex is at the beginning or end of the string unlike String.split
    val nonmatches = (("^" + re).r.findFirstIn(str).isDefined, (re + "$").r.findFirstIn(str).isDefined) match {
      case (true, true) => "" +: partialNonmatches :+ ""
      case (true, false) => "" +: partialNonmatches
      case (false, true) => partialNonmatches :+ ""
      case _ => partialNonmatches
    }
    assert(matches.size == nonmatches.size - 1)
    interleave(nonmatches, matches)
  }
}
