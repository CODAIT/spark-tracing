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

class EventTree(name: String = "", children: Seq[EventTree] = Seq.empty[EventTree]) extends Serializable {
  val isNull: Boolean = name.isEmpty && children.forall(_.isNull)
  val isDefined: Boolean = !isNull
  def debugPrint(indent: String = ""): Unit = {
    println(indent + "\"" + name + "\" (" + children.size + ")")
    children.foreach(_.debugPrint(indent + "    "))
  }
  override val toString: String = name + (if (!children.forall(_.isNull)) children.mkString("(", ", ", ")") else "")
  def apply(idx: Int): EventTree =
    if (idx == 0) new EventTree(name)
    else if (idx > children.size) new EventTree()
    else children(idx - 1)
  def apply(path: Seq[Int]): EventTree = if (path.isEmpty) this else this.apply(path.head).apply(path.tail)
  def getOption: Option[String] = if (name.nonEmpty) Some(name) else None
  def get: String = getOption.getOrElse(throw new IndexOutOfBoundsException("Tried to extract name from empty event tree"))
  def is(x: String): Boolean = getOption.contains(x)
  def isAny(x: Iterable[String]): Boolean = x.map(is).reduce(_ || _)
  def update(path: Seq[Int], replace: EventTree => EventTree): EventTree = path match {
    case head :: tail if children.nonEmpty => new EventTree(name, children.updated(head - 1, children(head - 1).update(tail, replace)))
    case head :: tail => throw new IndexOutOfBoundsException("Cannot replace child of leaf node")
    case _ => replace(this)
  }
  def update(path: Seq[Int], replacement: EventTree): EventTree = update(path, _ => replacement)
  def update(path: Seq[Int], replacement: String): EventTree = update(path, new EventTree(replacement))
}

object EventTree {
  def apply(s: String): EventTree = {
    """(?s)\A\s*([^(),]*)\s*(\((.*)\))?\s*\Z""".r.findFirstMatchIn(s) match {
      case Some(m) if m.group(2) != null && m.group(3).nonEmpty =>
        new EventTree(m.group(1).trim, Util.psplit(m.group(3)).map(x => EventTree(x.trim)))
      case Some(m) => new EventTree(m.group(1).trim)
      case None => throw new RuntimeException("Malformed event tree: " + s)
    }
  }
}
