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

import org.json4s._

class EventTree(name: String = "", children: Seq[EventTree] = Seq.empty[EventTree]) extends Serializable {
  val isNull: Boolean = name.isEmpty && children.forall(_.isNull)
  val isDefined: Boolean = !isNull
  val hasChildren: Boolean = !children.forall(_.isNull)
  def debugString(path: Seq[Int] = Seq.empty): String = {
    val pathstr = (if (hasChildren) path :+ 0 else path).mkString(".")
    val indent = 8 + path.length * 2 - pathstr.length
    pathstr + "." * indent + name + (
      if (hasChildren) "\n" + children.zipWithIndex.map(child => child._1.debugString(path :+ child._2 + 1)).mkString("\n")
      else ""
    )
  }
  override val toString: String = name + (if (hasChildren) children.mkString("(", ", ", ")") else "")
  def apply(idx: Int): EventTree =
    if (idx == 0) new EventTree(name)
    else if (idx > children.size) new EventTree()
    else children(idx - 1)
  def apply(path: Seq[Int]): EventTree = if (path.isEmpty) this else this.apply(path.head).apply(path.tail)
  def getOption: Option[String] = if (name.nonEmpty) Some(name) else None
  def get: String = getOption.getOrElse(throw new IndexOutOfBoundsException("Tried to extract name from empty event tree"))
  def is(x: String): Boolean = getOption.contains(x)
  def isAny(x: Iterable[String]): Boolean = x.map(is).foldLeft(false)(_ || _)
  def update(path: Seq[Int], replace: EventTree => EventTree): EventTree = path.toList match {
    case head :: tail if children.nonEmpty => new EventTree(name, children.updated(head - 1, children(head - 1).update(tail, replace)))
    case _ :: _ => throw new IndexOutOfBoundsException("Cannot replace child of leaf node")
    case Nil => replace(this)
  }
  def update(path: Seq[Int], replacement: EventTree): EventTree = update(path, _ => replacement)
  def update(path: Seq[Int], replacement: String): EventTree = update(path, new EventTree(replacement))
}

object EventTree {
  def apply(in: JValue): EventTree = in match {
    case JNull => new EventTree("null")
    case JString(s) => new EventTree(s)
    case JObject(fieldList) =>
      val fields = fieldList.toMap
      val name = fields.get("type") match {
        case None => throw new RuntimeException("JSON object missing \"type\" field")
        case Some(JString(s)) => s
        case _ => throw new RuntimeException("\"type\" field must be a string")
      }
      val children = fields.get("fields") match {
        case None => throw new RuntimeException("JSON object missing \"fields\" field")
        case Some(JArray(a)) => a.map(apply)
        case _ => throw new RuntimeException("\"fields\" field must be an array")
      }
      new EventTree(name, children)
    case JArray(a) => new EventTree("", a.map(apply))
    case _ => throw new RuntimeException("Unexpected JSON type: " + in.toString)
  }
  def apply(s: String, recurse: Boolean = false): EventTree = {
    """(?s)\A\s*([^(),]*)\s*(\((.*)\))?\s*\Z""".r.findFirstMatchIn(s) match {
      case Some(m) =>
        val children = if (m.group(2) != null) Util.psplit(m.group(3)) else Seq.empty[String]
        val childTrees =
          if (recurse) children.map(child => apply(child.trim, recurse))
          else children.map(child => new EventTree(child.trim))
        new EventTree(m.group(1).trim, childTrees)
      case None => throw new RuntimeException("Invalid case class syntax: " + s)
    }
  }
}
