package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._
import org.apache.spark.rdd.RDD

object Axes extends OutputBlockGenerator {
  override def apply(events: RDD[EventTree], resolve: ServiceMap) = new Axes(resolve)
}

class Axes(resolve: ServiceMap) extends OutputBlock {
  val name: String = "axes"
  val columns: Seq[String] = Seq("id", "name")
  def data: Iterable[Seq[Any]] = resolve.services.values.toSeq.sortBy(_.start).zipWithIndex.map(svc => Seq(svc._2, svc._1.id))
}
