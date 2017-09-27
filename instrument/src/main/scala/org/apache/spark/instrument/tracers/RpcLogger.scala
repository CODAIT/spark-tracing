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
