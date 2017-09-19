package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

trait StatSource {
  def name: String
  def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double]
}

object StatSource {
  def listenEvent(event: EventTree): Option[EventTree] = {
    if (event(3)(0).is("Fn") && event(3)(1)(0).is("org.apache.spark.SparkFirehoseListener.onEvent"))
      Some(event(3)(2)(1))
    else None
  }
}

trait StatCol {
  def name: String
  def colType: ColType
  def calculate(values: Map[Any, Double]): Any
}

class Stats(id: String, stats: Seq[StatSource], cols: Seq[StatCol], events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "stat:" + id
  val columns: Seq[(String, ColType)] = Seq("stat" -> Str) ++ cols.map(col => col.name -> col.colType)
  def data: Iterable[Seq[Any]] = {
    stats.map(stat => {
      val values = stat.extract(events, resolve)
      stat.name +: cols.map(_.calculate(values))
    })
  }
}
