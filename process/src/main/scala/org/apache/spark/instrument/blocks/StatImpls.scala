package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

object StatJVMStart extends StatSource {
  val name: String = "JVM start time"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    Util.spanLength(events, "JVMStart", row => resolve.processes(row(1).get.get).services.head.id).map(row => (row._1, row._2 / 1000.0))
}

object StatInstrOver extends StatSource {
  val name: String = "Instrument overhead"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(_(3)(0).is("InstrumentOverhead"))
      .map(row => resolve.processes(row(1).get.get).services.head.id -> row(3)(1).get.get.toInt / 1000.0)
      .collect.toMap
}

object StatRPCCount extends StatSource {
  val name: String = "RPCs sent"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    events.filter(_(3)(0).is("RPC")).map(row => resolve.processes(row(1).get.get).services.head.id -> 1.0)
      .reduceByKey(_ + _).collect.toMap
}

object StatExecLife extends StatSource {
  val name: String = "Executor lifetime"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    Util.timeDelta(events, row => row(3)(0).is("SpanEnd") && row(3)(2).is("JVMStart"), _(3).is("MainEnd"), _(1).get.get)
      .map(row => (resolve.processes(row._1).services.head.id, row._2 / 1000.0)).asInstanceOf[Map[Any, Double]]
}

object StatTaskLength extends StatSource {
  val name: String = "Task duration"
  override def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double] =
    ???
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

