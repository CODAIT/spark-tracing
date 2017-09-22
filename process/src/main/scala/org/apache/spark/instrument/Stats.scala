package org.apache.spark.instrument

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object StatUtils {
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
  def fnArgs(event: EventTree, name: String): Option[EventTree] = {
    if (event(3)(0).is("Fn") && event(3)(1)(0).is(name)) Some(event(3)(2))
    else None
  }
  def isDagEvent(ev: EventTree, name: String): Boolean =
    fnArgs(ev, "org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive").isDefined && ev(3)(2)(1)(0).is(name)
  // FIXME I would prefer to include the following function inline where it's used below, but then Spark's closure processor
  // FIXME tries to serialize the whole Stats class rather than just the fields that are required for this function.  Is
  // FIXME there a way to tell Spark to only serialize what's necessary?
  def traceEvents(events: RDD[EventTree], trace: Int, resolve: ServiceMap): RDD[EventTree] =
    events.filter(event => resolve.processes(event(1).get.get).trace.id == trace)
}

trait StatSource {
  def name: String
  def extract(events: RDD[EventTree], resolve: ServiceMap): Map[Any, Double]
}

trait StatCol {
  def name: String
  def colType: ColType
  def calculate(values: Map[Any, Double]): Any
}

class Stats(id: String, stats: Seq[StatSource], cols: Seq[StatCol], events: RDD[EventTree], resolve: ServiceMap) extends OutputBlock {
  val name: String = "stat:" + id
  val columns: Seq[(String, ColType)] = Seq("trace" -> Num, "stat" -> Str) ++ cols.map(col => col.name -> col.colType)
  def data: Iterable[Seq[Any]] = {
    resolve.traces.keys.toSeq.sorted.flatMap { trace => // toSeq ensures the rows remain in the order they are processed here
      val traceEvents = StatUtils.traceEvents(events, trace, resolve)
      stats.map { stat =>
        val values = stat.extract(traceEvents, resolve)
        trace +: stat.name +: cols.map(_.calculate(values))
      }
    }
  }
}
