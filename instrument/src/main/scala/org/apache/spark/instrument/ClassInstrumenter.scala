package org.apache.spark.instrument

import java.io.ByteArrayInputStream
import java.lang.instrument.ClassFileTransformer
import java.security.ProtectionDomain
import javassist._
import org.apache.spark.instrument.actions._

class ClassInstrumenter() extends ClassFileTransformer {
  val targets = Seq(
    //new Call("org.apache.spark.util.Utils$", "initDaemon"), // Debug
    new RpcIntercept,
    new EndpointInfo,
    new MainLogger
    //new ExitLogger // Not working: NoClassDefFound.  Try using a shutdown hook?
  )

  def instrumentClass(cls: CtClass): CtClass = {
    //cls.getDeclaredConstructors
    cls.getDeclaredMethods.foreach(method => {
      //println(method.getLongName)
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
