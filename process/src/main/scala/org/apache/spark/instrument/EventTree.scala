package org.apache.spark.instrument

import scala.collection.mutable.ListBuffer

trait EventTree {
  def apply(idx: Int): EventTree
  def is(s: String): Boolean
  def get: String
  def format(spec: String): String = ??? // TODO
  //def prepend(item: EventTree): EventTree
  //def append(item: EventTree): EventTree
  //def rename(name: String): EventTree
}

case object EventNull extends EventTree {
  override def toString: String = "<empty>"
  override def apply(idx: Int): EventTree = EventNull
  override def is(s: String): Boolean = false
  override def get: String = throw new IndexOutOfBoundsException("Cannot get value of null node")
}

case class EventLeaf(value: String) extends EventTree {
  override def toString: String = value
  override def apply(idx: Int): EventTree = EventNull
  override def is(s: String): Boolean = value == s
  override def get: String = value
}

case class EventBranch(children: Seq[EventTree]) extends EventTree {
  override def toString: String = children.head + children.tail.mkString("(", ", ", ")")
  override def apply(idx: Int): EventTree = children(idx)
  override def is(s: String): Boolean = false
  override def get: String = throw new IndexOutOfBoundsException("Cannot get value of non-leaf node")
}

object EventTree {
  private def psplit(s: String): Seq[String] = { // TODO vars and mutables
    var lvl = 0
    val cur = new StringBuilder
    val ret = new ListBuffer[String]
    s.foreach(c => {
      if (c == '(') lvl += 1
      else if ( c == ')') lvl -= 1
      if (c == ',' && lvl == 0) {
        ret += cur.toString
        cur.clear
      }
      else cur += c
    })
    if (cur.nonEmpty) ret += cur.toString
    ret
  }
  def apply(s: String): EventTree = {
    """\s*([^()]*)(\((.*)\))?\s*""".r.findFirstMatchIn(s) match {
      case Some(m) if m.group(2) != null && m.group(3) != "" => EventBranch(EventLeaf(m.group(1)) +: psplit(m.group(3)).map(x => EventTree(x)))
      case _ => EventLeaf(s)
    }
  }
}