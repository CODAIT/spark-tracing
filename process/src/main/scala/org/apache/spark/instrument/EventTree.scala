package org.apache.spark.instrument

trait EventTree {
  def apply(idx: Int): EventTree
  def apply(path: Seq[Int]): EventTree = if (path.isEmpty) this else this.apply(path.head).apply(path.tail)
  def get: Option[String]
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
  override def apply(idx: Int): EventTree = if (idx == 0) this else EventNull
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
  def apply(s: String): EventTree = {
    """\s*([^()]*)(\((.*)\))?\s*""".r.findFirstMatchIn(s) match {
      case Some(m) if m.group(2) != null =>
        EventBranch(EventLeaf(m.group(1)) +: Util.psplit(m.group(3)).map(x => EventTree(x.trim)))
      case _ => EventLeaf(s)
    }
  }
}
