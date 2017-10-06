package org.apache.spark.instrument

import org.scalatest._

class BlockTest extends FlatSpec with Matchers {
  private val in = Spark.sc.parallelize(SampleEvents.run).map(EventTree(_))
  private val sm = new ServiceMap(Seq(in), Set("3 1 filterMe".r))

  "Axes block" should "process data" in {
    new Axes(sm).data.toSeq shouldBe Seq(Seq("1 1 sparkDriver"), Seq("2 1 sparkExecutor"))
  }

  "RPCs block" should "process data" in {
    new RPCs(in, sm).data.toSeq.sortBy(_.head.asInstanceOf[String].toLong) shouldBe Seq (
      Seq("14", "1 1 sparkDriver", "2 1 sparkExecutor", "Request"),
      Seq("15", "2 1 sparkExecutor", "1 1 sparkDriver", "Response")
    )
  }

  "Events block" should "process data" in {
    val format = Map(
      SampleEvents.dagSchedEv -> new FormatSpec("sched($1.0($1.1))"),
      SampleEvents.blockUpdEv -> new FormatSpec("block($3)"),
      "add" -> new FormatSpec("$1 + $2 = $r")
    )
    new Events(in, sm, format).data.toSeq
      .sortBy(_.head.asInstanceOf[String].toLong).toList shouldBe Seq(
      Seq("16", "1 1 sparkDriver", "sched(ExecutorAdded(Exec 1))"),
      Seq("20", "1 1 sparkDriver", "sched(JobSubmitted(Job 1))"),
      Seq("22", "1 1 sparkDriver", "sched(BeginEvent(Task(1, 1)))"),
      Seq("24", "2 1 sparkExecutor", "block(Y)"),
      Seq("28", "2 1 sparkExecutor", "block(Z)"),
      Seq("32", "1 1 sparkDriver", "sched(CompletionEvent(Task(1, 1)))"),
      Seq("34", "1 1 sparkDriver", "sched(BeginEvent(Task(1, 2)))"),
      Seq("40", "1 1 sparkDriver", "sched(CompletionEvent(Task(1, 2)))"),
      Seq("42", "2 1 sparkExecutor", "MainEnd"),
      Seq("43", "2 1 sparkExecutor", "InstrumentOverhead(4)"),
      Seq("48", "1 1 sparkDriver", "MainEnd"),
      Seq("50", "1 1 sparkDriver", "InstrumentOverhead(2)")
    )
  }

  "Spans block" should "process data" in {
    val res = new Spans(in, sm, Map("add" -> new FormatSpec("$1 + $2 = $r"))).data.toSeq
      .sortBy(_.head.asInstanceOf[String].toInt) shouldBe Seq(
      Seq("1", "4", "1 1 sparkDriver", "JVMStart"),
      Seq("2", "4", "2 1 sparkExecutor", "JVMStart"),
      Seq("30", "31", "2 1 sparkExecutor", "1 + 2 = 3")
    )
  }

  "TimeRange block" should "process data" in {
    new TimeRange(in, sm).data.toSeq shouldBe Seq(Seq(1), Seq(50))
  }
}
