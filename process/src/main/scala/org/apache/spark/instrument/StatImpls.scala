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

import org.apache.spark.rdd.RDD

object StatJVMStart extends StatSource {
  val name: String = "JVM start time"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    StatUtils.spanLength(events, "JVMStart", row => resolve.mainService(row(1).get).map(_.id)).map(row => (row._1, row._2 / 1000.0))
}

object StatInstrOver extends StatSource {
  val name: String = "Instr Overhead"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(_(3)(0).is("InstrumentOverhead"))
      .flatMap(row => resolve.mainService(row(1).get).map(_.id -> row(3)(1).get.toInt / 1000.0))
      .collect.toMap
}

object StatRPCCount extends StatSource {
  val name: String = "RPCs sent"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(_(3)(0).is("RPC")).flatMap(row => resolve.mainService(row(1).get).map(_.id -> 1.0))
      .reduceByKey(_ + _).collect.toMap
}

object StatExecLife extends StatSource {
  val name: String = "Executor lifetime"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    StatUtils.timeDelta(events, row => row(3)(0).is("SpanStart") && row(3)(2).is("JVMStart"), _(3).is("MainEnd"), _(1).get)
      .flatMap(row => resolve.mainService(row._1).filter(_.name == "sparkExecutor").map(_.id -> row._2 / 1000.0))
      .asInstanceOf[Map[Any, Double]]
}

object StatTaskLength extends StatSource {
  val name: String = "Task duration"
  private def taskTuple(ev: EventTree) = {
    val arg1 = ev(3)(2)(1)(1)
    (arg1(1).get, arg1(2).get)
  }
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    StatUtils.timeDelta(events, StatUtils.isDagEvent(_, "BeginEvent"), StatUtils.isDagEvent(_, "CompletionEvent"), taskTuple)
      .mapValues(_ / 1000.0).asInstanceOf[Map[Any, Double]]
}

object StatJVMs extends StatSource {
  val name: String = "JVMs"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(row => row(3)(0).is("SpanStart") && row(3)(2).is("JVMStart")).map(_(1).get -> 0.0).collect.toMap
}

object StatExecs extends StatSource {
  val name: String = "Executors"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(StatUtils.isDagEvent(_, "ExecutorAdded")).map(_(3)(2)(1)(1).get -> 0.0).collect.toMap
}

object StatJobs extends StatSource {
  val name: String = "Jobs"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(StatUtils.isDagEvent(_, "JobSubmitted")).map(_(3)(2)(1)(1).get -> 0.0).collect.toMap
}

object StatTasks extends StatSource {
  val name: String = "Tasks"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] = StatTaskLength.extract(events, resolve)
}

object StatBlockUpdates extends StatSource {
  val name: String = "Block updates"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(StatUtils.fnArgs(_, "org.apache.spark.storage.BlockManagerMaster.updateBlockInfo").isDefined)
      .map(_(2).get -> 0.0).collect.toMap
}

object ColCount extends StatCol {
  val name: String = "Count"
  val colType: ColType = Num
  def calculate(values: Map[Any, Double]): Int = values.size
}

class ColPercentile(p: Int) extends StatCol {
  val name: String = p.toString + "%"
  val colType: ColType = Num
  private def percentile(x: Iterable[Double], p: Int): Double = {
    val items = x.toSeq
    if (items.isEmpty) 0.0
    else items.sorted.apply((scala.math.ceil(p / 100.0 * x.size).toInt - 1).max(0))
  }
  def calculate(values: Map[Any, Double]): Double = percentile(values.values, p)
}

object ColArgMin extends StatCol {
  val name: String = "Min at"
  val colType: ColType = Str
  def calculate(values: Map[Any, Double]): String = values.toSeq.sortBy(_._2).headOption.map(_._1.toString).getOrElse("")
}

object ColArgMax extends StatCol {
  val name: String = "Max at"
  val colType: ColType = Str
  def calculate(values: Map[Any, Double]): String = values.toSeq.sortBy(_._2).lastOption.map(_._1.toString).getOrElse("")
}

