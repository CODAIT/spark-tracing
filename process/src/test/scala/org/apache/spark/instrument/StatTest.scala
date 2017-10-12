package org.apache.spark.instrument

import org.scalatest._

class StatTest extends FlatSpec with Matchers {
  private val in = Spark.sc.parallelize(SampleEvents.run).map(EventTree(_))
  private val sm = new ServiceMap(Seq(in), Set(".*filterMe".r))

  "JVMStart stat" should "compute" in {
    StatJVMStart.extract(in, sm).toMap shouldBe Map(
      "1 1 sparkDriver" -> 0.003,
      "2 1 sparkExecutor" -> 0.002
    )
  }

  "InstrOver stat" should "compute" in {
    StatInstrOver.extract(in, sm).toMap shouldBe Map(
      "1 1 sparkDriver" -> 0.002,
      "2 1 sparkExecutor" -> 0.004
    )
  }

  "RPCCount stat" should "compute" in {
    StatRPCCount.extract(in, sm).size shouldBe 2
  }

  "ExecLife stat" should "compute" in {
    StatExecLife.extract(in, sm).toMap shouldBe Map(
      "2 1 sparkExecutor" -> 0.04
    )
  }

  "TaskLength stat" should "compute" in {
    StatTaskLength.extract(in, sm).toMap shouldBe Map(
      ("1", "1") -> 0.01,
      ("1", "2") -> 0.006
    )
  }

  "JVMs stat" should "compute" in {
    StatJVMs.extract(in, sm).size shouldBe 3
  }

  "Execs stat" should "compute" in {
    StatExecs.extract(in, sm).size shouldBe 1
  }

  "Jobs stat" should "compute" in {
    StatJobs.extract(in, sm).size shouldBe 1
  }

  "Tasks stat" should "compute" in {
    StatTasks.extract(in, sm).size shouldBe 2
  }

  "BlockUpdates stat" should "compute" in {
    StatBlockUpdates.extract(in, sm).size shouldBe 2
  }

  "Count col" should "compute" in {
    ColCount.calculate(Map("a" -> 1, "b" -> 2, "c" -> 3)) shouldBe 3
  }

  "Percentile col" should "compute" in {
    ColCount.calculate(Map("a" -> 1, "b" -> 2, "c" -> 3)) shouldBe 3
  }

  "ArgMin col" should "compute" in {
    ColArgMin.calculate(Map("a" -> 1, "b" -> 2, "c" -> 3)) shouldBe "a"
  }

  "ArgMax col" should "compute" in {
    ColArgMax.calculate(Map("a" -> 1, "b" -> 2, "c" -> 3)) shouldBe "c"
  }
}
