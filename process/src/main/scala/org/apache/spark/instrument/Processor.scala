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
import java.io.{File, FileWriter}

import org.apache.spark.sql.SparkSession
import org.json4s.native.JsonMethods

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.matching.Regex

object Processor {
  def configProp(config: Config, name: String, require: Boolean = true): Option[String] =
    if (! config.hasPath("props." + name)) {
      if (require) throw new RuntimeException(s"Missing config property $name")
      else None
    }
    else Some(config.getString("props." + name))
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark trace processing").getOrCreate()
    import spark.implicits._
    require(args.length >= 1, "An input configuration file is required")
    val config = ConfigFactory.parseFile(new File(args.head))
    val traceFiles = configProp(config, "traceout").get +: args.tail
    val inputs = traceFiles.map { file =>
      val cur = spark.read.text(file).as[String].rdd.map(x => EventTree(JsonMethods.parse(x)))
      if (cur.isEmpty) cur
      else {
        val start = cur.map(_(2).get.toLong).min
        cur.map(_.update(Seq(2), t => new EventTree((t.get.toLong - start).toString)))
      }
    }
    val caseParse = CascadingCompare.apply[java.util.ArrayList[String]](config, "case-parse")
      .map(cond => CaseParseSpec(cond._1, cond._2.map(_.split("\\.").toSeq.map(_.toInt))))
    val transforms = Formatter(config)
    val eventFilters = CascadingCompare.apply[Boolean](config, "filters")
      .map(filter => EventFilterSpec(filter._1, filter._2))
    val serviceFilters =
      if (config.hasPath("remove-services")) config.getStringList("remove-services").asScala.map(_.r).toSet
      else Set.empty[Regex]
    val resolve = new ServiceMap(inputs, serviceFilters)
    val in = inputs.reduce(_.union(_)).map(Transforms.applyCaseParse(caseParse))
      .filter(Transforms.applyFilters(eventFilters)).cache
    configProp(config, "mode", false) match {
      case Some("process") | None =>
        val blocks: Set[OutputBlock] = Set(
          new TimeRange(in, resolve),
          new Axes(resolve),
          new RPCs(in, resolve),
          new Events(in, resolve, transforms),
          new Spans(in, resolve, transforms),
          new Stats("count", Seq(StatJVMs, StatExecs, StatJobs, StatTasks, StatBlockUpdates), Seq(ColCount), in, resolve),
          new Stats("dist", Seq(StatJVMStart, StatExecLife, StatTaskLength,  StatRPCCount, StatInstrOver),
            Seq(0, 25, 50, 75, 100).map(new ColPercentile(_)) ++ Seq(ColArgMin, ColArgMax), in, resolve)
        )
        val out = new Output(new File(configProp(config, "result").get))
        blocks.foreach(block => out.addBlock(block))
        out.close()
      case Some("dump") =>
        val out = new FileWriter(new File(configProp(config, "result").get))
        out.write(in.map(_.debugString()).collect.mkString("\n" + "=" * 80 + "\n"))
        out.close()
      case Some(x) => throw new RuntimeException(s"Unknown mode: $x")
    }
  }
}
