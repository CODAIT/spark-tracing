package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

class Events(events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "events"
  val columns: Seq[(String, ColType)] = Seq("time" -> Time, "location" -> Str, "content" -> Str)
  def data: Iterable[Seq[Any]] = events.filter(row => {
    val evType = row(3)(0)
    !evType.is("SpanStart") && !evType.is("SpanEnd") && !evType.is("RPC") && !evType.is("Service")
  }).map(row => Seq(row(2).get, resolve.process(row(1).get).services.head.id, row(3).toString)).collect
}
