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

trait TraceTarget
case class EventT(fn: String) extends TraceTarget
case class VariableT(fn: String, varname: String) extends TraceTarget
case object OtherT extends TraceTarget

object TraceTarget {
  def fromConf(pkg: String, config: Config): TraceTarget = {
    if (! Set("event", "span", "local").contains(config.getString("type"))) OtherT
    else {
      val className = pkg + "." + config.getString("class")
      val methodName = config.getString("method")
      val fullName =
        if (className.endsWith("." + methodName)) className // Constructor
        else className + "." + methodName
      config.getString("type") match {
        case "event" | "span" => EventT(fullName)
        case "local" => VariableT(fullName, config.getString("variable"))
        case _ => OtherT
      }
    }
  }
  def fromEvent(ev: EventTree): TraceTarget = ev(0).getOption match {
    case Some("SpanStart") | Some("Fn") => EventT(ev(1)(0).get)
    case Some("LocalVariable") => VariableT(ev(1)(0).get, ev(2).get)
    case _ => OtherT
  }
}

class FormatSpec(ttype: TraceTarget, spec: String) extends Serializable {
  trait SpecElem
  case class Literal(s: String) extends SpecElem
  case class Extract(path: Seq[Int]) extends SpecElem
  val parts: Seq[SpecElem] = {
    Util.tokenize(spec, """\$(\d+|[a-z])(\.\d+)*""").zipWithIndex.map { case (token: String, idx: Int) =>
      if (idx % 2 == 0) Literal(token)
      else {
        val rawPath = token.substring(1).split("\\.")
        val path = (ttype, rawPath.headOption) match {
          case (EventT(_), Some("r")) => "3" +: rawPath.tail
          case (_, Some(x)) if x.matches("^[a-z]$") =>
            throw new IllegalArgumentException(s"Invalid specifier $x in format specification")
          case (EventT(_), Some(_)) => "2" +: rawPath
          case (_, Some(_)) => rawPath // VariableT
          case _ => throw new IllegalArgumentException("Empty extraction path in format specification")
        }
        Extract(path.map(_.toInt))
      }
    }
  }
  override val toString: String = spec
  def format(ev: EventTree): String = parts.map {
    case Literal(str) => str
    case Extract(path) => ev(path)
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

class Formatter(transforms: Map[TraceTarget, FormatSpec]) extends Serializable {
  def format(ev: EventTree): String = {
    transforms.get(TraceTarget.fromEvent(ev)).map(_.format(ev)).getOrElse(ev.toString)
  }
}

object Formatter {
  def apply(config: Config) = new Formatter({
    def confToFormatPair(pkg: String, conf: Config): (TraceTarget, Option[String]) = {
      val fmtString = if (conf.hasPath("format")) Some(conf.getString("format")).filter(_ != "") else None
      TraceTarget.fromConf(pkg, conf) -> fmtString
    }
    if (!config.hasPath("targets")) Map.empty[TraceTarget, FormatSpec]
    else config.getObject("targets").keySet().asScala.flatMap(key =>
      config.getConfig("targets").getObjectList("\"" + key + "\"").asScala.map(_.toConfig).map(confToFormatPair(key, _))
    ).filter(x => x._1 != OtherT && x._2.isDefined).map(x => (x._1, new FormatSpec(x._1, x._2.get))).toMap // Can't use mapValues because it returns a non-serializable view
  })
}

object Transforms {
  def applyFilters(eventFilters: Seq[EventFilterSpec])(ev: EventTree): Boolean =
    eventFilters.filter(_.matches(ev)).lastOption.forall(_.value)

  def applyCaseParse(parse: Seq[CaseParseSpec])(ev: EventTree): EventTree =
    parse.foldLeft(ev)((ev, pa) => pa.apply(ev))
}
