package org.apache.spark.instrument

import org.scalatest._

class ConfigTest extends FlatSpec with Matchers {
  "Config" should "load configuration" in {
    Config.get[String]("result") shouldBe Some("/tmp/spark-trace.out")
    Config.targets().keys shouldBe Set("org.apache.spark")
    Config.targets()("org.apache.spark").size shouldBe 1
    Config.targets()("org.apache.spark").head.getString("type") shouldBe "rpc"
    Config.trackOverhead shouldBe true
  }
}
