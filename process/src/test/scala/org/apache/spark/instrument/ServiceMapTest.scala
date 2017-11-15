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

class ServiceMapTest extends FlatSpec with Matchers {
  private val in = SampleEvents.services.map(trace => Spark.sc.parallelize(trace).map(EventTree(_, true)))

  "ServiceMap" should "construct mappings" in {
    val sm = new ServiceMap(in, Set())

    sm.traces.size shouldBe 2
    sm.processes.size shouldBe 9
    sm.services.size shouldBe 15

    val t1 = sm.trace(0)
    t1.id shouldBe 0
    t1.processes.size shouldBe 4
    val p1 = t1.processes.head
    p1.trace.id shouldBe 0
    p1.trace.processes.size shouldBe 4
    p1.services.size should be > 0
    val s1 = p1.services.head
    s1.process.services.size should be > 0

    val p2 = sm.process("af5470e0-4dfe-4e8e-ba92-59efd4266acb")
    p2.trace.id shouldBe 1
    p2.uuid shouldBe "af5470e0-4dfe-4e8e-ba92-59efd4266acb"
    p2.services.size shouldBe 2

    val s2 = sm.service("/10.50.108.70:38638")
    s2.host shouldBe "10.50.108.70"
    s2.port shouldBe 38638
    s2.start shouldBe 1505917121956L
    s2.id shouldBe "10.50.108.70 38638 sparkDriver"
    s2.process.uuid shouldBe "b5b55c94-21bc-4f90-a027-39c4591cc65f"
    s2.process.trace.id shouldBe 1
  }

  it should "filter services" in {
    val unfiltered = new ServiceMap(in, Set())
    val executorFiltered = new ServiceMap(in, Set(".*sparkExecutor".r))
    val fetcherFiltered = new ServiceMap(in, Set(".*driverPropsFetcher".r))

    unfiltered.filterRPC("/10.50.108.70:41246", "/10.50.108.70:35766") shouldBe true
    unfiltered.filterRPC("/10.50.108.70:35766", "/10.50.108.70:41250") shouldBe true
    unfiltered.filterRPC("/10.50.108.70:41246", "/10.50.108.70:41250") shouldBe true
    executorFiltered.filterRPC("/10.50.108.70:41246", "/10.50.108.70:35766") shouldBe true
    executorFiltered.filterRPC("/10.50.108.70:35766", "/10.50.108.70:41250") shouldBe false
    executorFiltered.filterRPC("/10.50.108.70:41246", "/10.50.108.70:41250") shouldBe false
    fetcherFiltered.filterRPC("/10.50.108.70:41246", "/10.50.108.70:35766") shouldBe false
    fetcherFiltered.filterRPC("/10.50.108.70:35766", "/10.50.108.70:41250") shouldBe true
    fetcherFiltered.filterRPC("/10.50.108.70:41246", "/10.50.108.70:41250") shouldBe false

    unfiltered.mainService("3bcf41dd-fcb9-40f4-9ce0-da5337598517").map(_.id) shouldBe Some("10.50.108.70 35766 sparkDriver")
    unfiltered.mainService("77152954-d105-46ca-b6cf-3a32c76e42a6").map(_.id) shouldBe Some("10.50.108.70 41246 driverPropsFetcher")
    executorFiltered.mainService("3bcf41dd-fcb9-40f4-9ce0-da5337598517").map(_.id) shouldBe Some("10.50.108.70 35766 sparkDriver")
    fetcherFiltered.mainService("3bcf41dd-fcb9-40f4-9ce0-da5337598517").map(_.id) shouldBe Some("10.50.108.70 35766 sparkDriver")
    executorFiltered.mainService("77152954-d105-46ca-b6cf-3a32c76e42a6").map(_.id) shouldBe Some("10.50.108.70 41246 driverPropsFetcher")
    fetcherFiltered.mainService("77152954-d105-46ca-b6cf-3a32c76e42a6").map(_.id) shouldBe Some("10.50.108.70 41250 sparkExecutor")

    new ServiceMap(in, Set(".*sparkExecutor".r, ".*driverPropsFetcher".r)).mainService("77152954-d105-46ca-b6cf-3a32c76e42a6") shouldBe None
  }
}
