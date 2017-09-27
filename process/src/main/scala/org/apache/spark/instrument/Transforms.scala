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

import com.typesafe.config._
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

class FormatSpec(spec: String) extends Serializable {
  trait SpecElem
  case class Literal(s: String) extends SpecElem
  case class Extract(path: Seq[Int]) extends SpecElem
  val parts: Seq[SpecElem] = {
    Util.tokenize(spec, """\$\d+(\.\d+)*""").zipWithIndex.map { case (token: String, idx: Int) =>
      if (idx % 2 == 0) Literal(token)
      else Extract(token.substring(1).split("\\.").map(_.toInt))
    }
  }
  override def toString: String = spec
  def format(ev: EventTree): String = parts.map {
    case l: Literal => l.s
    case e: Extract => ev(e.path).toString
  }.mkString
}

class EventFilterSpec(condStrs: Seq[String], val value: Boolean) extends Serializable {
  val comparisons: Map[String, (String, String) => Boolean] = Map(
    "=" -> (_ == _),
    "!=" -> (_ != _),
    "in" -> ((elem, list) => list.split(",\\s*").contains(elem))
  )
  case class Cond(field: Seq[Int], comp: (String, String) => Boolean, value: String) {
    def badPath(ev: EventTree) =
      throw new IndexOutOfBoundsException("The following EventTree does not have path " + field.mkString(".") + ": " + ev)
    def check(ev: EventTree): Boolean = comp(ev.apply(field).get.getOrElse(badPath(ev)), value)
  }
  override def toString: String = condStrs.mkString(" && ") + " => " + value
  val conditions: Seq[Cond] = condStrs.map { cond =>
    val parts = cond.split("\\s+", 3)
    require(parts.length == 3, "Invalid condition string")
    Cond(
      parts(0).split("\\.").map(_.toInt),
      comparisons.getOrElse(parts(1), throw new IllegalArgumentException("Unknown comparison \"" + parts(1) + "\"")),
      parts(2)
    )
  }
  def matches(ev: EventTree): Boolean = conditions.forall(_.check(ev))
}

object Transforms {
  def fmtEvent(ev: EventTree, transforms: Map[String, FormatSpec]): String = {
    if (!ev(0).is("Fn")) ev.toString
    else transforms.get(ev(1)(0).get.get).map(_.format(ev(2))).getOrElse(ev.toString)
  }

  def getTransforms(config: Config): Map[String, FormatSpec] = {
    def isTransformable(tracer: Config) = Set("event", "span").contains(tracer.getString("type"))
    def confToFormatPair(pkg: String, conf: Config): (String, Option[String]) = {
      val className = pkg + "." + conf.getString("class")
      val methodName = conf.getString("method")
      val fullName =
        if (className.endsWith("." + methodName)) className // Constructor
        else className + "." + methodName
      val fmtString = if (conf.hasPath("format")) Some(conf.getString("format")).filter(_ != "") else None
      fullName -> fmtString
    }
    config.getObject("targets").keySet().asScala.flatMap(key =>
      config.getConfig("targets").getObjectList("\"" + key + "\"").asScala.map(_.toConfig)
        .filter(isTransformable).map(confToFormatPair(key, _))
    ).filter(_._2.isDefined).toMap.map(x => (x._1, new FormatSpec(x._2.get))) // Can't use mapValues because it returns a non-serializable view
  }

  def getEventFilters(config: Config): Seq[EventFilterSpec] = {
    def recursiveFilters(config: Map[String, AnyRef]): Seq[(List[String], Boolean)] = { // HOCON, why?
      config.flatMap { case (key: String, value: AnyRef) =>
        value match {
          case b: java.lang.Boolean => Seq((if (key == "default") List.empty else List(key)) -> b.booleanValue)
          case c: java.util.Map[String, AnyRef] => recursiveFilters(c.asScala.toMap).map(item => (key :: item._1, item._2))
          case _ => throw new IllegalArgumentException("All filter values must be boolean")
        }
      }.toSeq
    }
    recursiveFilters(config.getValue("filters").unwrapped.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap)
      .sortBy(_._1.length)
      .map(spec => new EventFilterSpec(spec._1, spec._2))
  }

  def applyFilters(events: RDD[EventTree], eventFilters: Seq[EventFilterSpec]): RDD[EventTree] = {
    events.filter(row => eventFilters.filter(_.matches(row)).lastOption.forall(_.value))
  }
}
