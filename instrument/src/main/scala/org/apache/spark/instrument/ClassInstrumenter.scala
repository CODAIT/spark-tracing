package org.apache.spark.instrument

import java.io.ByteArrayInputStream
import java.lang.instrument.ClassFileTransformer
import java.security.ProtectionDomain
import javassist._
import org.apache.spark.instrument.tracers._
import com.typesafe.config._
import scala.collection.JavaConverters._

class ClassInstrumenter(val config: Config) extends ClassFileTransformer {
  private def getTracer(pkg: String, target: Config): Tracer = {
    target.getString("type") match {
      case "event" => new Event(pkg + "." + target.getString("class"), target.getString("method"))
      case "span" => new Span(pkg + "." + target.getString("class"), target.getString("method"))
      case "rpc" => new RpcLogger
      case x => throw new RuntimeException(s"Unknown tracer type $x")
    }
  }

  val packages: Set[String] = config.getObject("targets").keySet.asScala.map(_.replace(".", "/")).toSet

  val targets: Seq[Tracer] = Seq(new MainLogger) ++ config.getObject("targets").asScala.flatMap(pkg => {
    config.getConfigList(s"""targets."${pkg._1}"""").asScala.map(target => getTracer(pkg._1, target))
  })

  def instrumentClass(cls: CtClass): CtClass = {
    cls.getDeclaredBehaviors.foreach(method => {
      targets.find(_.matches(method)).foreach(target => {
        println(s"Instrumenting ${method.getLongName} with ${target.toString}")
        try target.apply(method)
        catch { case e: Throwable => e.printStackTrace() }
      })}
    )
    cls
  }

  override def transform(loader: ClassLoader, name: String, curClass: Class[_], protectionDomain: ProtectionDomain,
    buffer: Array[Byte]): Array[Byte] = {
    if (packages.foldLeft(false)((x, cur) => x || name.startsWith(cur))) {
      val targetClass = ClassPool.getDefault.makeClass(new ByteArrayInputStream(buffer))
      instrumentClass(targetClass).toBytecode()
    }
    else null
  }
}
