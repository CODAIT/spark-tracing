package org.apache.spark.instrument

import java.io.ByteArrayInputStream
import java.lang.instrument.ClassFileTransformer
import java.security.ProtectionDomain
import javassist._
import org.apache.spark.instrument.tracers._

class ClassInstrumenter() extends ClassFileTransformer {
  val targets = Seq(
    new MainLogger,
    //new ExitLogger,
    //new Listener, // Doesn't log anything, possibly because no listeners are implemented?
    //new Event("java.lang.Shutdown", "exit"), // FIXME NoClassDefFoundError why?
    new DagScheduler,
    new TaskScheduler,
    new Blocks,
    new Shuffles,
    new Rpcs,
    /*new Event("org.apache.spark.scheduler.DAGSchedulerEventProcessLoop", "doOnReceive"),
    new Event("org.apache.spark.scheduler.TaskSchedulerImpl", "submitTasks"),
    new Event("org.apache.spark.scheduler.TaskSchedulerImpl", "taskSetFinished"),
    new Event("org.apache.spark.storage.BlockManager", "getBlockData"),
    new Event("org.apache.spark.storage.BlockManager", "get"),
    new Event("org.apache.spark.storage.BlockManager", "doPut"),
    new Event("org.apache.spark.storage.BlockManager", "dropFromMemory"),
    new Event("org.apache.spark.storage.BlockManager", "removeBlock"),
    new Event("org.apache.spark.storage.BlockManagerMaster", "registerBlockManager"),
    new Event("org.apache.spark.storage.BlockManagerMaster", "updateBlockInfo"),
    new Event("org.apache.spark.storage.BlockManagerMaster", "removeBlock"),
    new Event("org.apache.spark.storage.BlockManagerMaster", "removeRdd"),
    new Event("org.apache.spark.storage.BlockManagerMaster", "removeShuffle"),
    new Event("org.apache.spark.storage.BlockManagerMaster", "removeBroadcast"),
    new Event("org.apache.spark.network.netty.NettyBlockTransferService", "fetchBlocks"),
    new Event("org.apache.spark.network.netty.NettyBlockTransferService", "uploadBlock"),
    new Event("org.apache.spark.shuffle.sort.SortShuffleManager", "registerShuffle"),
    new Event("org.apache.spark.shuffle.sort.SortShuffleManager", "unregisterShuffle"),
    new Event("org.apache.spark.MapOutputTrackerMaster", "registerShuffle"),
    new Event("org.apache.spark.MapOutputTrackerMaster", "unregisterShuffle"),*/

    new Call("org.apache.spark.rpc.RpcEnv$", "create"), // Instrumenting NettyRpcEnvFactory.create doesn't give results, for some reason
    new Call("org.apache.spark.deploy.yarn.ApplicationMaster", "org$apache$spark$deploy$yarn$ApplicationMaster$$waitForSparkDriver"),
    new Call("org.apache.spark.deploy.yarn.ApplicationMaster", "org$apache$spark$deploy$yarn$ApplicationMaster$$registerAM"),
    //new Call("org.apache.spark.deploy.yarn.Client", "prepareLocalResources"),
    //new Call("org.apache.spark.deploy.yarn.Client", "submitApplication"),
    new Call("org.apache.spark.deploy.yarn.Client", "org$apache$spark$deploy$yarn$Client$$distribute$1"),
    //new ClientLogger,
    new Call("org.apache.spark.deploy.yarn.YarnAllocator", "allocateResources"),
    new Call("org.apache.spark.deploy.yarn.ExecutorRunnable", "startContainer"),
    new Call("org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend", "waitForApplication"),
    new Call("org.apache.spark.SparkContext", "createSparkEnv"),
    new Call("org.apache.spark.SparkContext", "SparkContext"),
    new Call("org.apache.spark.metrics.MetricsSystem", "start"),
    new Call("org.apache.spark.scheduler.TaskSchedulerImpl", "waitBackendReady")
    //new Call("org.apache.spark.deploy.yarn.Client", "Client") // Doesn't construct properly for some reason, so we get NPEs.
    // TODO Need FetchDriverProps in CoarseGrainedExecutorBackend.run
  )

  def instrumentClass(cls: CtClass): CtClass = {
    cls.getDeclaredBehaviors.foreach(method => {
     //if (method.getDeclaringClass.getName.startsWith("org.apache.spark.deploy.yarn.Client")) println(method.getLongName + " | " + method.getDeclaringClass.getName + " | " + method.getName)
      targets.find(_.matches(method)).foreach(target => {
        println(s"Instrumenting ${method.getLongName} with ${target.toString}")
        target.apply(method)
      })}
    )
    cls
  }

  override def transform(loader: ClassLoader, name: String, curClass: Class[_], protectionDomain: ProtectionDomain,
    buffer: Array[Byte]): Array[Byte] = {
    if (name.startsWith("org/apache/spark") || name.startsWith("java/lang/Shutdown")) try {
      val targetClass = ClassPool.getDefault.makeClass(new ByteArrayInputStream(buffer))
      instrumentClass(targetClass).toBytecode()
    }
    catch {
      case e: Throwable => {
        e.printStackTrace()
        null
      }
    }
    else null
  }
}
