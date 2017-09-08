package org.apache.spark.instrument.tracers

import java.net.SocketAddress
import javassist._
import org.apache.spark.instrument._
import org.apache.spark.network.client.TransportClient
import org.apache.spark.rpc.RpcEnvConfig

case class Service(name: String, host: SocketAddress) extends TraceEvent
case class RPC(src: SocketAddress, dst: SocketAddress, payload: Any) extends TraceEvent

object RpcLogger {
  var curService: Option[String] = None
  def newEndpoint(config: RpcEnvConfig): Unit = curService = Some(config.name)
  def log(client: TransportClient, msg: Any): Unit = TraceWriter.runAsOverhead {
    if (curService.isDefined) {
      TraceWriter.log(System.currentTimeMillis, Service(curService.get, client.getChannel.localAddress))
      curService = None
    }
    TraceWriter.log(System.currentTimeMillis, RPC(client.getChannel.remoteAddress, client.getChannel.localAddress, msg))
  }
  def log(client: scala.Function0[TransportClient], msg: Any): Unit = ()
}

class RpcLogger() extends Tracer {
  private def isRpc(method: CtBehavior) = check(method, "org.apache.spark.rpc.netty.NettyRpcEnv", "deserialize")
  private def isCreate(method: CtBehavior) = check(method, "org.apache.spark.rpc.netty.NettyRpcEnvFactory", "create")
  override def matches(method: CtBehavior): Boolean = {
    isRpc(method) || isCreate(method)
  }
  override def apply(method: CtBehavior): Unit = {
    val report =
      if (isRpc(method)) functionCall(this.getClass.getCanonicalName, "log", Seq("$1", "$_"))
      else functionCall(this.getClass.getCanonicalName, "newEndpoint", Seq("$1"))
    method.insertAfter(report)
  }
}
