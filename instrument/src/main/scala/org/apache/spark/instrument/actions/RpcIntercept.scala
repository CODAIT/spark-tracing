package org.apache.spark.instrument.actions

import java.net.SocketAddress
import javassist.CtMethod

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}
import org.apache.spark.network.client.TransportClient

case class SparkHost(name: String, host: String, port: Int)
case class RPC(src: SocketAddress, dst: SocketAddress, payload: Any)

object RpcIntercept {
  def log(client: TransportClient, msg: Any): Unit = {
    TraceWriter.log(System.currentTimeMillis, RPC(client.getChannel.remoteAddress, client.getChannel.localAddress, msg))
  }
  def log(client: scala.Function0[TransportClient], msg: Any): Unit = () // Conveniently, this seems to always duplicate the one above
}

class RpcIntercept() extends MethodInstrumentation {
  override def matches(method: CtMethod): Boolean = {
    method.getDeclaringClass.getName == "org.apache.spark.rpc.netty.NettyRpcEnv" && method.getName == "deserialize"
  }
  override def apply(method: CtMethod): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq("$1", "$_"))
    method.insertAfter(report)
  }
}
