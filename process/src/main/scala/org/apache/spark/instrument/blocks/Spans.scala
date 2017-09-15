package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

object Spans extends OutputBlockGenerator {
  override def apply(events: RDD[EventTree], resolve: ServiceMap) = new Spans(events, resolve)
}

class Spans(events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "spans"
  val columns: Seq[String] = Seq("start", "end", "location", "content")
  def data: Iterable[Seq[Any]] = ???
}
