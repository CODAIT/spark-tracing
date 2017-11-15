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

import com.typesafe.config.Config
import scala.collection.JavaConverters._

sealed trait Cond {
  def check(ev: EventTree): Boolean
}

case class ConstCond(value: Boolean) extends Cond {
  override def check(ev: EventTree): Boolean = value
  override val toString: String = value.toString
}

case class EvalCond(field: Seq[Int], comp: (EventTree, String) => Boolean, value: String, compStr: String) extends Cond {
  override def check(ev: EventTree): Boolean = comp(ev.apply(field), value)
  override val toString: String = field.mkString(".") + " " + compStr + " " + value
}

object Cond {
  private val comparisons: Map[String, (EventTree, String) => Boolean] = Map(
    "=" -> (_.toString == _),
    "==" -> (_.toString == _),
    "!=" -> (_.toString != _),
    "in" -> ((elem, list) => list.split(",\\s*").contains(elem.toString)),
    "exists" -> ((elem, _) => elem.isDefined),
    "~" -> ((s, re) => re.r.findFirstMatchIn(s.toString).isDefined),
    "matches" -> ((s, re) => re.r.findFirstMatchIn(s.toString).isDefined)
  )
  def apply(s: String): Cond = {
    def pathsplit(desc: String) = desc.split("\\.").map(_.toInt)
    def getcomp(comp: String) = comparisons.get(comp) match {
      case Some(c) => c
      case None => throw new RuntimeException(s"Unknown comparison predicate: $comp")
    }
    val parts: Seq[String] = s.split("\\s+", 3)
    parts match {
      case Seq("default") => ConstCond(true)
      case Seq(path, pred) => EvalCond(pathsplit(path), getcomp(pred), "", pred)
      case Seq(path, pred, comp) => EvalCond(pathsplit(path), getcomp(pred), comp, pred)
    }
  }
  def seqCompare(a: (Seq[Cond], Any), b: (Seq[Cond], Any)): Boolean = {
    if (a._1.isEmpty) true
    else if (b._1.isEmpty) false
    else if (a._1.length < b._1.length) true
    else if (a._1.length > b._1.length) false
    else (a._1.last, b._1.last) match {
      case (_: ConstCond, _: EvalCond) => true
      case _ => false
    }
  }
}

object CascadingCompare {
  def apply[T](config: Config, key: String): Seq[(Seq[Cond], T)] = {
    def recursiveResolve(config: Map[String, AnyRef]): Map[Seq[String], T] = { // HOCON, why?
      config.flatMap { case (key: String, value: AnyRef) =>
        value match {
          case c: java.util.Map[String @unchecked, AnyRef @unchecked]  =>
            recursiveResolve(c.asScala.toMap).map(item => (key +: item._1, item._2))
          case v: T => Map(Seq(key) -> v) // FIXME Erasure issues
          case v => throw new RuntimeException("Invalid type for value: " + v)
        }
      }
    }
    if (!config.hasPath(key)) Seq.empty
    else recursiveResolve(config.getValue(key).unwrapped.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap)
      .map(spec => (spec._1.map(Cond.apply), spec._2)).toSeq.sortWith(Cond.seqCompare)
  }
}
