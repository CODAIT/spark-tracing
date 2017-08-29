package org.apache.spark.instrument.actions

import javassist.CtMethod

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}
import org.apache.spark.rpc.RpcEnvConfig

case class EndpointName(name: String, bind: String, advert: String, port: Int)

object EndpointInfo {
  def log(config: RpcEnvConfig): Unit = {
    TraceWriter.log(System.currentTimeMillis, EndpointName(config.name, config.bindAddress, config.advertiseAddress, config.port))
  }
}

class EndpointInfo extends MethodInstrumentation {
  override def matches(method: CtMethod): Boolean = {
    method.getDeclaringClass.getName == "org.apache.spark.rpc.netty.NettyRpcEnvFactory" && method.getName == "create"
  }
  override def apply(method: CtMethod): Unit = {
    method.insertBefore(functionCall(this.getClass.getCanonicalName, "log", Seq("$1")))
  }
}
