package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

class TimeRange(events: RDD[EventTree]) extends OutputBlock {
  val name: String = "timerange"
  val columns: Seq[(String, ColType)] = Seq("time" -> Time)
  def data: Iterable[Seq[Any]] = {
    val times = events.map(_(2).get.get.toLong)
    Seq(Seq(times.min), Seq(times.max))
  }
}
