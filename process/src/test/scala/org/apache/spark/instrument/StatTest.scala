/* Copyright 2017 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.instrument

import org.scalatest._

class StatTest extends FlatSpec with Matchers {
  private val in = Spark.sc.parallelize(SampleEvents.run).map(EventTree(_, true))
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
