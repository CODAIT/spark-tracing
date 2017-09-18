package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

class RPCs(events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "rpcs"
  val columns: Seq[(String, ColType)] = Seq("time" -> Time, "origin" -> Str, "destination" -> Str, "content" -> Str)
  // FIXME collecting after mapping yields NotSerializable
  def data: Iterable[Seq[Any]] = events.filter(_(3)(0).is("RPC")).map(row => {
    val ev = row(3)
    Seq(row(2).get, resolve.service(ev(1).get).id, resolve.service(ev(2).get).id, ev(3).toString)
  }).collect
}
