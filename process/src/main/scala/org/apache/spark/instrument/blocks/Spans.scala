package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

class Spans(events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "spans"
  val columns: Seq[(String, ColType)] = Seq("start" -> Time, "end" -> Time, "location" -> Str, "content" -> Str)
  def data: Iterable[Seq[Any]] = {
    val starts = events.filter(_(3)(0).is("SpanStart")).map(row => (row(3)(1).get, (row(1).get, row(2).get, row(3)(2))))
    val ends = events.filter(_(3)(0).is("SpanEnd")).map(row => (row(3)(1).get, row(2).get))
    val all = starts.join(ends).map(_._2) // RDD of ((JVM ID, start time, event), end time)
    all.map(span => Seq(span._1._2, span._2, resolve.process(span._1._1).services.head.id, span._1._3.toString)).collect
  }
}
