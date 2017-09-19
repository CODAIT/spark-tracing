package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

class Events(events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "events"
  val columns: Seq[(String, ColType)] = Seq("time" -> Time, "location" -> Str, "content" -> Str)
  private val nonEvents: Set[String] = Set("SpanStart", "SpanEnd", "RPC", "Service")
  def data: Iterable[Seq[Any]] = events.filter(row => row(3)(0).get.exists(!nonEvents.contains(_)))
    .map(row => Seq(row(2).get.get, resolve.process(row(1).get.get).services.head.id, row(3).toString)).collect
}
