package org.apache.spark.instrument.tracers

import javassist._
import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class BlockOperation(op: String, args: Seq[Any])

object Blocks {
  val funcs = Map(
    "org.apache.spark.storage.BlockManager" ->
      Set("getBlockData", "get", "doPut", "dropFromMemory", "removeBlock"),
    "org.apache.spark.storage.BlockManagerMaster" ->
      Set("registerBlockManager", "updateBlockInfo", "removeBlock", "removeRdd", "removeShuffle", "removeBroadcast"),
    "org.apache.spark.network.netty.NettyBlockTransferService" ->
      Set("fetchBlocks", "uploadBlock")
  )
  def log(op: String, args: Array[Any]): Unit = {
    TraceWriter.log(System.currentTimeMillis, BlockOperation(op, args.toSeq))
  }
}

class Blocks extends MethodInstrumentation {
  override def matches(method: CtBehavior): Boolean =
    Blocks.funcs.get(method.getDeclaringClass.getName).exists(_.contains(method.getName))

  override def apply(method: CtBehavior): Unit = {
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq(str(method.getName), "$args"))
    method.insertBefore(report)
  }
}
