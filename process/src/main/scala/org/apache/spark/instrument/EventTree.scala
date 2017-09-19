package org.apache.spark.instrument

import scala.collection.mutable.ListBuffer

trait EventTree {
  def apply(idx: Int): EventTree
  def get: Option[String]
  def format(spec: String): String = ??? // TODO
  def is(x: String): Boolean = get.contains(x)
}

case object EventNull extends EventTree {
  override def toString: String = "<empty>"
  override def apply(idx: Int): EventTree = EventNull
  override def get: Option[String] = None
}

case class EventLeaf(value: String) extends EventTree {
  override def toString: String = value
  override def apply(idx: Int): EventTree = EventNull
  override def get: Option[String] = Some(value)
}

case class EventBranch(children: Seq[EventTree]) extends EventTree {
  override def toString: String = children.head + children.tail.mkString("(", ", ", ")")
  override def apply(idx: Int): EventTree = if (idx >= children.size) EventNull else children(idx)
  override def get: Option[String] = None
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
        ret += cur.toString.trim
        cur.clear
      }
      else cur += c
    })
    if (cur.nonEmpty) ret += cur.toString.trim
    ret
  }
  def apply(s: String): EventTree = {
    """\s*([^()]*)(\((.*)\))?\s*""".r.findFirstMatchIn(s) match {
      case Some(m) if m.group(2) != null && m.group(3) != "" => EventBranch(EventLeaf(m.group(1)) +: psplit(m.group(3)).map(x => EventTree(x)))
      case _ => EventLeaf(s)
    }
  }
}
