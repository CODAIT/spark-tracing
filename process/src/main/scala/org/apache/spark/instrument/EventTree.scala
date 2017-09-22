package org.apache.spark.instrument

import scala.collection.mutable.ListBuffer

trait EventTree {
  def apply(idx: Int): EventTree
  def get: Option[String]
  def format(spec: String): String = ??? // TODO
  def is(x: String): Boolean = get.contains(x)
  def update(path: Seq[Int], replace: EventTree => EventTree): EventTree
  def update(path: Seq[Int], replacement: EventTree): EventTree = update(path, _ => replacement)
  def update(path: Seq[Int], replacement: String): EventTree = update(path, EventLeaf(replacement))
}

case object EventNull extends EventTree {
  override def toString: String = "<empty>"
  override def apply(idx: Int): EventTree = EventNull
  override def get: Option[String] = None
  def update(path: Seq[Int], replace: EventTree => EventTree): EventTree = throw new IndexOutOfBoundsException("Cannot replace an empty node")
}

case class EventLeaf(value: String) extends EventTree {
  override def toString: String = value
  override def apply(idx: Int): EventTree = EventNull
  override def get: Option[String] = Some(value)
  def update(path: Seq[Int], replace: EventTree => EventTree): EventTree = path match {
    case head :: tail => throw new IndexOutOfBoundsException("Cannot replace child of a leaf node")
    case _ => replace(this)
  }
}

case class EventBranch(children: Seq[EventTree]) extends EventTree {
  override def toString: String = children.head + children.tail.mkString("(", ", ", ")")
  override def apply(idx: Int): EventTree = if (idx >= children.size) EventNull else children(idx)
  override def get: Option[String] = None
  def update(path: Seq[Int], replace: EventTree => EventTree): EventTree = path match {
    case head :: tail => EventBranch(children.updated(head, children(head).update(tail, replace)))
    case _ => replace(this)
  }
}

object EventTree {
  /*private def psplit(s: String): Seq[String] = {
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
  def apply(s: String): EventTree = {
    """\s*([^()]*)(\((.*)\))?\s*""".r.findFirstMatchIn(s) match {
      case Some(m) if m.group(2) != null && m.group(3) != "" =>
        EventBranch(EventLeaf(m.group(1)) +: psplit(m.group(3)).map(EventTree(_)))
      case _ => EventLeaf(s)
    }
  }
}
