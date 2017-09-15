package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

object RPCs extends OutputBlockGenerator {
  override def apply(events: RDD[EventTree], resolve: ServiceMap) = new RPCs(events, resolve)
}

class RPCs(events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "rpcs"
  val columns: Seq[String] = Seq("time", "origin", "destination", "content")
  def data: Iterable[Seq[Any]] = ???
}
