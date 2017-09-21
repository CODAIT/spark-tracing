package org.apache.spark.instrument.blocks

import org.apache.spark.instrument._

class Axes(resolve: ServiceMap) extends OutputBlock {
  val name: String = "axes"
  val columns: Seq[(String, ColType)] = Seq("name" -> Str)
  def data: Iterable[Seq[Any]] = resolve.services.values.toSeq.sortBy(row => (row.process.trace.id, row.start)).map(row => Seq(row.id))
}
