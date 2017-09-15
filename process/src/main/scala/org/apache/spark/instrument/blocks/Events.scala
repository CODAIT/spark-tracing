package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

object Events extends OutputBlockGenerator {
  override def apply(events: RDD[EventTree], resolve: ServiceMap) = new Events(events, resolve)
}

class Events(events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "events"
  val columns: Seq[String] = Seq("time", "location", "content")
  def data: Iterable[Seq[Any]] = ???
}
