package org.apache.spark.instrument.tracers

import javassist.CtMethod

import org.apache.spark.instrument.{MethodInstrumentation, TraceWriter}

case class BlockOperation(op: String, id: Any)

object BlockManager {
  val managerFuncs = Map(
    "getBlockData" -> ("GetData", 1),
    "get" -> ("Get", 1),
    "doPut" -> ("Put", 1),
    "dropFromMemory" -> ("Free", 1),
    "removeBlock" -> ("Delete", 1)
  )
  val masterFuncs = Map(
    "registerBlockManager" -> ("Register", 1),
    "updateBlockInfo" -> ("UpdateBlockInfo", 2),
    "removeBlock" -> ("RemoveBlock", 1),
    "removeRdd" -> ("RemoveRDD", 1),
    "removeShuffle" -> ("RemoveShuffle", 1),
    "removeBroadcast" -> ("RemoveBroadcast", 1)
  )
  def log(op: String, id: Any): Unit = {
    TraceWriter.log(System.currentTimeMillis, BlockOperation(op, id))
  }
  def log(op: String, id: Int): Unit = log(op, id.asInstanceOf[Any])
  def log(op: String, id: Long): Unit = log(op, id.asInstanceOf[Any])
}

class BlockManager extends MethodInstrumentation {
  import BlockManager._
  def isManager(method: CtMethod): Boolean =
    check(method, "org.apache.spark.storage.BlockManager") && managerFuncs.contains(method.getName)
  def isMaster(method: CtMethod): Boolean =
    check(method, "org.apache.spark.storage.BlockManagerMaster") && masterFuncs.contains(method.getName)
  override def matches(method: CtMethod): Boolean = isManager(method) || isMaster(method)

  override def apply(method: CtMethod): Unit = {
    val funcDesc =
      if (isManager(method)) managerFuncs(method.getName)
      else masterFuncs(method.getName)
    val report = functionCall(this.getClass.getCanonicalName, "log", Seq(str(funcDesc._1), "$" + funcDesc._2.toString))
    method.insertBefore(report)
  }
}
