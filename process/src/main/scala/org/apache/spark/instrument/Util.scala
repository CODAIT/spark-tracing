package org.apache.spark.instrument

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object Util {
  def timeDelta[T: ClassTag](events: RDD[EventTree], start: EventTree => Boolean, end: EventTree => Boolean,
    partition: EventTree => T, eventTime: EventTree => Long = _(2).get.get.toLong): Map[T, Long] = {
    val starts = events.filter(start).map(row => partition(row) -> eventTime(row))
    val ends = events.filter(end).map(row => partition(row) -> eventTime(row))
    starts.join(ends).map(row => (row._1, row._2._2 - row._2._1)).collect.toMap
  }
  def spanLength(events: RDD[EventTree], spanName: String, partition: EventTree => String = _(3)(1).get.get): Map[String, Long] = {
    val matches = events.filter(_(3)(2).is(spanName))
    val starts = matches.filter(_(3)(0).is("SpanStart")).map(row => partition(row) -> row(2).get.get.toLong)
    val ends = matches.filter(_(3)(0).is("SpanEnd")).map(row => partition(row) -> row(2).get.get.toLong)
    starts.join(ends).map(row => row._1 -> (row._2._2 - row._2._1)).collect.toMap
  }
}
