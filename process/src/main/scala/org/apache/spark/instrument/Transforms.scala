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
import scala.collection.JavaConverters._

class FormatSpec(spec: String) extends Serializable {
  trait SpecElem
  case class Literal(s: String) extends SpecElem
  case class Extract(path: Seq[Int]) extends SpecElem
  val parts: Seq[SpecElem] = {
    Util.tokenize(spec, """\$(\d+|r)(\.\d+)*""").zipWithIndex.map { case (token: String, idx: Int) =>
      if (idx % 2 == 0) Literal(token)
      else {
        val rawPath = token.substring(1).split("\\.")
        val path = rawPath.headOption match {
          case Some("r") => "3" +: rawPath.tail
          case Some(_) => "2" +: rawPath
          case None => throw new IllegalArgumentException("Empty extraction path in format specification")
        }
        Extract(path.map(_.toInt))
      }
    }
  }
  override def toString: String = spec
  def format(ev: EventTree): String = parts.map {
    case l: Literal => l.s
    case e: Extract => ev(e.path).toString
  }.mkString
}

case class CaseParseSpec(conds: Seq[Cond], toParse: Seq[Seq[Int]]) extends Serializable {
  def apply(ev: EventTree): EventTree =
    if (!conds.forall(_.check(ev))) ev
    else toParse.foldLeft(ev)((ev, path) => ev.update(path, x => EventTree(x.get)))
}

case class EventFilterSpec(conds: Seq[Cond], value: Boolean) extends Serializable {
  override def toString: String = conds.mkString(" && ") + " => " + value
  def matches(ev: EventTree): Boolean = conds.forall(_.check(ev))
}

object Transforms {
  def fmtEvent(ev: EventTree, transforms: Map[String, FormatSpec]): String = {
    if (!ev(0).is("Fn")) ev.toString
    else transforms.get(ev(1)(0).get).map(_.format(ev)).getOrElse(ev.toString)
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
    if (!config.hasPath("targets")) Map.empty[String, FormatSpec]
    else config.getObject("targets").keySet().asScala.flatMap(key =>
      config.getConfig("targets").getObjectList("\"" + key + "\"").asScala.map(_.toConfig)
        .filter(isTransformable).map(confToFormatPair(key, _))
    ).filter(_._2.isDefined).toMap.map(x => (x._1, new FormatSpec(x._2.get))) // Can't use mapValues because it returns a non-serializable view
  }

  def applyFilters(eventFilters: Seq[EventFilterSpec])(ev: EventTree): Boolean =
    eventFilters.filter(_.matches(ev)).lastOption.forall(_.value)

  def applyCaseParse(parse: Seq[CaseParseSpec])(ev: EventTree): EventTree =
    parse.foldLeft(ev)((ev, pa) => pa.apply(ev))
}
