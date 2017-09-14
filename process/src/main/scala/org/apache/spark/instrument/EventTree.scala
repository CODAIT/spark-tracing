package org.apache.spark.instrument

import scala.collection.mutable.ListBuffer

trait EventTree {
  def apply(idx: Int): EventTree
}

case class EventLeaf(value: String) extends EventTree {
  override def toString: String = value
  override def apply(idx: Int): EventTree = throw new IndexOutOfBoundsException("Cannot get child of leaf node")
}

case class EventBranch(name: EventLeaf, children: Seq[EventTree]) extends EventTree {
  override def toString: String = name + children.mkString("(", ", ", ")")
  override def apply(idx: Int): EventTree = idx match {
    case 0 => name
    case x => children(x - 1)
  }
}

object EventTree {
  private def psplit(s: String): Seq[String] = { // TODO
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
      case Some(m) if m.group(2) != null && m.group(3) != "" => EventBranch(EventLeaf(m.group(1)), psplit(m.group(3)).map(x => EventTree(x)))
      case _ => EventLeaf(s)
    }
  }
}